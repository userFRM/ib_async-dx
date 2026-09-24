//! ib_async's `FlexReport`: download, save, load and read Flex statements.

use std::collections::BTreeSet;
use std::env::VarError;
use std::io::Read;
use std::net::Ipv6Addr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;

use crate::error::{Error, Result};
use crate::objects::{DynamicObject, DynamicValue};
use crate::util::py_number;

/// The log target ib_async's `flexreport` module logs to.
const LOG: &str = "ib_async.flexreport";

/// The Flex web service's request address: ib_async's `FLEXREPORT_URL`.
pub const FLEXREPORT_URL: &str =
    "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?";

/// A Flex statement, as the XML the web service returns: ib_async's
/// `FlexReport`.
#[derive(Clone, Debug, Default)]
pub struct FlexReport {
    /// The statement's XML: `data`.
    pub data: Vec<u8>,
}

/// An element of a parsed report: the ElementTree `Element` ib_async's
/// `FlexReport.root` holds.
#[derive(Clone, Debug)]
pub struct Element {
    /// The element's name, `{namespace}name` when it has a namespace: `tag`.
    pub tag: String,
    /// The attributes in document order, named as `tag` is: `attrib`.
    pub attrib: IndexMap<String, String>,
    /// The text before the first child element, if any: `text`.
    pub text: Option<String>,
    /// The text after this element's end and before the next element, if
    /// any: `tail`.
    pub tail: Option<String>,
    /// The child elements in document order: the element's items.
    pub children: Vec<Element>,
}

impl Element {
    /// This element and every element below it, in document order, whose
    /// tag is `tag`; `None` or `"*"` takes every one: ElementTree's `iter`.
    pub fn iter<'a>(&'a self, tag: Option<&'a str>) -> impl Iterator<Item = &'a Element> {
        let tag = tag.filter(|t| *t != "*");
        let mut stack = vec![self];
        std::iter::from_fn(move || {
            let e = stack.pop()?;
            stack.extend(e.children.iter().rev());
            Some(e)
        })
        .filter(move |e| tag.is_none_or(|t| e.tag == t))
    }

    /// The first child whose tag is `tag`: ElementTree's `find` for a plain
    /// tag.
    pub fn find(&self, tag: &str) -> Option<&Element> {
        self.children.iter().find(|e| e.tag == tag)
    }

    /// Every child whose tag is `tag`, in document order: ElementTree's
    /// `findall` for a plain tag.
    pub fn findall(&self, tag: &str) -> Vec<&Element> {
        self.children.iter().filter(|e| e.tag == tag).collect()
    }
}

impl FlexReport {
    /// The web service's request address: `IB_FLEXREPORT_URL` when set, else
    /// [`FLEXREPORT_URL`], if it has a scheme and a host; else `Err(Flex)`:
    /// ib_async's `FlexReport.get_url`.
    pub fn get_url() -> Result<String> {
        let url = match std::env::var("IB_FLEXREPORT_URL") {
            Ok(url) => Some(url),
            Err(VarError::NotPresent) => Some(FLEXREPORT_URL.to_owned()),
            Err(VarError::NotUnicode(_)) => None,
        };
        url.filter(|u| is_valid_url(u)).ok_or_else(|| {
            Error::Flex(
                "Invalid URL, please check that env variable IB_FLEXREPORT_URL is set correctly."
                    .into(),
            )
        })
    }

    /// Downloads the statement of query `query_id` with `token`, asking again
    /// every second while it is being generated, with no bound: ib_async's
    /// `FlexReport.download`. A refused request is `Err(Flex)` stating
    /// `"{ErrorCode}: {ErrorMessage}"`; a network failure is `Err(Io)`.
    pub fn download(token: &str, query_id: &str) -> Result<FlexReport> {
        let base_url = Self::get_url()?;
        let tls = native_tls::TlsConnector::new().map_err(io_error)?;
        let agent = ureq::AgentBuilder::new()
            .tls_connector(Arc::new(tls))
            .build();
        let answer = parse(&fetch(
            &agent,
            &format!("{base_url}t={token}&q={query_id}&v=3"),
        )?)?;
        let (code, base_url) = reference(&answer)?;
        log::info!(target: LOG, "Statement is being prepared...");
        loop {
            std::thread::sleep(Duration::from_secs(1));
            let data = fetch(&agent, &format!("{base_url}?q={code}&t={token}&v=3"))?;
            if !in_progress(&parse(&data)?)? {
                log::info!(target: LOG, "Statement retrieved.");
                return Ok(FlexReport { data });
            }
            log::info!(target: LOG, "still working...");
        }
    }

    /// Reads a report saved as XML; a file that is not XML is `Err(Flex)`:
    /// ib_async's `FlexReport.load`.
    pub fn load(path: impl AsRef<Path>) -> Result<FlexReport> {
        let report = FlexReport {
            data: std::fs::read(path)?,
        };
        // ib_async parses the file as it loads it.
        report.root()?;
        Ok(report)
    }

    /// Writes the report's XML to `path`: ib_async's `FlexReport.save`.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        Ok(std::fs::write(path, &self.data)?)
    }

    /// The tags of the elements that have attributes, which `extract` can
    /// read: ib_async's `FlexReport.topics`.
    pub fn topics(&self) -> Result<BTreeSet<String>> {
        let root = self.root()?;
        Ok(root
            .iter(None)
            .filter(|e| !e.attrib.is_empty())
            .map(|e| e.tag.clone())
            .collect())
    }

    /// The attributes of every element tagged `topic`, in document order,
    /// each value read as a number where `parse_numbers` is set and Python's
    /// `float()` reads it: ib_async's `FlexReport.extract`.
    pub fn extract(&self, topic: &str, parse_numbers: bool) -> Result<Vec<DynamicObject>> {
        let root = self.root()?;
        Ok(root
            .iter(Some(topic))
            .map(|e| {
                e.attrib
                    .iter()
                    .map(|(k, v)| {
                        let value = if parse_numbers {
                            py_number(v)
                        } else {
                            DynamicValue::Str(v.clone())
                        };
                        (k.clone(), value)
                    })
                    .collect()
            })
            .collect())
    }

    /// The parsed report; invalid XML is `Err(Flex)`: ib_async's
    /// `FlexReport.root`.
    pub fn root(&self) -> Result<Element> {
        parse(&self.data)
    }
}

/// Parses XML as ElementTree's `fromstring` does, a document type
/// declaration included.
fn parse(data: &[u8]) -> Result<Element> {
    let text = std::str::from_utf8(data).map_err(|e| Error::Flex(e.to_string()))?;
    let options = roxmltree::ParsingOptions {
        allow_dtd: true,
        ..Default::default()
    };
    let doc = roxmltree::Document::parse_with_options(text, options)
        .map_err(|e| Error::Flex(e.to_string()))?;
    Ok(element(doc.root_element()))
}

/// Builds ElementTree's element: text split by a comment or a processing
/// instruction joins up, as ElementTree drops both.
fn element(node: roxmltree::Node) -> Element {
    let tag = node.tag_name();
    let mut e = Element {
        tag: qualified(tag.namespace(), tag.name()),
        attrib: node
            .attributes()
            .map(|a| (qualified(a.namespace(), a.name()), a.value().to_owned()))
            .collect(),
        text: None,
        tail: None,
        children: Vec::new(),
    };
    for child in node.children() {
        if child.is_element() {
            e.children.push(element(child));
        } else if child.is_text() {
            let slot = match e.children.last_mut() {
                Some(last) => &mut last.tail,
                None => &mut e.text,
            };
            slot.get_or_insert_default()
                .push_str(child.text().unwrap_or_default());
        }
    }
    e
}

/// ElementTree's name: `{namespace}name`, or `name` alone.
fn qualified(namespace: Option<&str>, name: &str) -> String {
    match namespace {
        Some(ns) => format!("{{{ns}}}{name}"),
        None => name.to_owned(),
    }
}

/// Whether `urllib.parse.urlparse` finds a scheme and a host in `url`, a URL
/// it refuses counting as neither: ib_async's `is_valid_url`. The NFKC check
/// `urlsplit` makes on a host that is not ASCII is not made.
fn is_valid_url(url: &str) -> bool {
    let url: String = url
        .trim_start_matches(|c| c <= ' ')
        .chars()
        .filter(|c| !matches!(c, '\t' | '\r' | '\n'))
        .collect();
    let Some((scheme, rest)) = url.split_once(':') else {
        return false;
    };
    let is_scheme = scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c));
    let Some(rest) = rest.strip_prefix("//").filter(|_| is_scheme) else {
        return false;
    };
    let netloc = &rest[..rest.find(['/', '?', '#']).unwrap_or(rest.len())];
    !netloc.is_empty()
        && match (netloc.contains('['), netloc.contains(']')) {
            (false, false) => true,
            (true, true) => is_bracketed_netloc(netloc),
            _ => false,
        }
}

/// `urlsplit`'s `_check_bracketed_netloc` and `_check_bracketed_host`: the
/// host in brackets is an IPv6 address, with an optional scope, or an
/// IPvFuture one.
fn is_bracketed_netloc(netloc: &str) -> bool {
    let host_port = netloc.rsplit_once('@').map_or(netloc, |(_, h)| h);
    let host = match host_port.split_once('[') {
        Some(("", bracketed)) => {
            let (host, port) = bracketed.split_once(']').unwrap_or((bracketed, ""));
            if !port.is_empty() && !port.starts_with(':') {
                return false;
            }
            host
        }
        Some(_) => return false,
        None => host_port.split_once(':').map_or(host_port, |(h, _)| h),
    };
    match host.strip_prefix('v') {
        Some(future) => future.split_once('.').is_some_and(|(hex, rest)| {
            !hex.is_empty() && hex.chars().all(|c| c.is_ascii_hexdigit()) && !rest.is_empty()
        }),
        None => {
            let addr = match host.split_once('%') {
                None => host,
                Some((addr, scope)) if !scope.is_empty() && !scope.contains('%') => addr,
                Some(_) => return false,
            };
            addr.parse::<Ipv6Addr>().is_ok()
        }
    }
}

/// The reference code and the statement address in the web service's answer
/// to a request, or `Err(Flex)` with the error it states.
fn reference(answer: &Element) -> Result<(String, String)> {
    let text = |tag: &str| answer.find(tag).map(|e| py_str(&e.text).to_owned());
    if answer.find("Status").and_then(|e| e.text.as_deref()) == Some("Success") {
        let code = text("ReferenceCode").ok_or_else(|| missing("ReferenceCode"))?;
        let url = text("Url").ok_or_else(|| missing("Url"))?;
        Ok((code, url))
    } else {
        let code = text("ErrorCode").unwrap_or_default();
        let message = text("ErrorMessage").unwrap_or_default();
        Err(Error::Flex(format!("{code}: {message}")))
    }
}

/// Whether a statement answer says the statement is still being generated;
/// any other message in its place is `Err(Flex)`.
fn in_progress(answer: &Element) -> Result<bool> {
    let first = answer
        .children
        .first()
        .ok_or_else(|| Error::Value("child index out of range".into()))?;
    if first.tag != "code" {
        return Ok(false);
    }
    match first.text.as_deref() {
        Some(msg) if msg.starts_with("Statement generation in progress") => Ok(true),
        msg => Err(Error::Flex(msg.unwrap_or("None").to_owned())),
    }
}

/// An element's text as ib_async formats it into a string: `None` when it
/// has none.
fn py_str(text: &Option<String>) -> &str {
    text.as_deref().unwrap_or("None")
}

/// A successful answer without the element ib_async asserts it has.
fn missing(tag: &str) -> Error {
    Error::Value(format!("the Flex answer has no {tag}"))
}

/// Fetches `url`'s body.
fn fetch(agent: &ureq::Agent, url: &str) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    agent
        .get(url)
        .call()
        .map_err(io_error)?
        .into_reader()
        .read_to_end(&mut data)?;
    Ok(data)
}

/// A network failure, as ib_async's `urlopen` raises an `OSError`.
fn io_error(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    std::io::Error::other(e).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use DynamicValue::{Float, Int, Str};

    const REPORT: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="trades" type="AF">
<FlexStatements count="1">
<FlexStatement accountId="U1234567" fromDate="20240102" toDate="20240102" whenGenerated="20240103;083015">
<Trades>
<!-- two trades -->
<Trade accountId="U1234567" currency="USD" symbol="AAPL" conid="265598" quantity="100" tradePrice="185.64" ibCommission="-1" tradeDate="20240102" notes="" />
<Trade accountId="U1234567" currency="USD" symbol="MSFT" conid="272093" quantity="-50" tradePrice="370.6" ibCommission="-0.35" tradeDate="20240102" notes="P" />
</Trades>
<CashReport />
</FlexStatement>
</FlexStatements>
</FlexQueryResponse>
"#;

    fn temp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("ib_async_dx_{}_{name}", std::process::id()))
    }

    fn tags<'a>(it: impl Iterator<Item = &'a Element>) -> Vec<&'a str> {
        it.map(|e| e.tag.as_str()).collect()
    }

    fn tree(xml: &str) -> Element {
        FlexReport {
            data: xml.as_bytes().to_vec(),
        }
        .root()
        .unwrap()
    }

    // Each expectation is what ib_async 2.1.0's FlexReport gives on REPORT.
    #[test]
    fn saved_report_reads_as_ib_async_reads_it() {
        let path = temp("saved.xml");
        FlexReport {
            data: REPORT.as_bytes().to_vec(),
        }
        .save(&path)
        .unwrap();
        let report = FlexReport::load(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(report.data, REPORT.as_bytes());

        let topics: Vec<_> = report.topics().unwrap().into_iter().collect();
        assert_eq!(
            topics,
            [
                "FlexQueryResponse",
                "FlexStatement",
                "FlexStatements",
                "Trade"
            ]
        );

        let trades = report.extract("Trade", true).unwrap();
        assert_eq!(trades.len(), 2);
        let first: Vec<_> = trades[0].iter().map(|(k, v)| (k.as_str(), v)).collect();
        assert_eq!(
            first,
            [
                ("accountId", &Str("U1234567".into())),
                ("currency", &Str("USD".into())),
                ("symbol", &Str("AAPL".into())),
                ("conid", &Int(265598)),
                ("quantity", &Int(100)),
                ("tradePrice", &Float(185.64)),
                ("ibCommission", &Int(-1)),
                ("tradeDate", &Int(20240102)),
                ("notes", &Str(String::new())),
            ]
        );
        assert_eq!(trades[1]["quantity"], Int(-50));
        assert_eq!(trades[1]["ibCommission"], Float(-0.35));
        assert_eq!(trades[1]["notes"], Str("P".into()));

        let raw = report.extract("Trade", false).unwrap();
        assert_eq!(raw[0]["conid"], Str("265598".into()));
        assert_eq!(raw[0]["tradePrice"], Str("185.64".into()));
        let statement = report.extract("FlexStatement", true).unwrap();
        assert_eq!(statement[0]["whenGenerated"], Str("20240103;083015".into()));
        assert!(report.extract("Nope", true).unwrap().is_empty());
    }

    #[test]
    fn root_is_elementtrees_tree() {
        let root = tree(REPORT);
        assert_eq!(root.tag, "FlexQueryResponse");
        let attrib: Vec<_> = root.attrib.iter().collect();
        assert_eq!(
            attrib,
            [
                (&"queryName".to_owned(), &"trades".to_owned()),
                (&"type".to_owned(), &"AF".to_owned())
            ]
        );
        assert_eq!(root.text.as_deref(), Some("\n"));
        assert_eq!(root.tail, None);

        let all = [
            "FlexQueryResponse",
            "FlexStatements",
            "FlexStatement",
            "Trades",
            "Trade",
            "Trade",
            "CashReport",
        ];
        assert_eq!(tags(root.iter(None)), all);
        assert_eq!(tags(root.iter(Some("*"))), all);
        assert_eq!(tags(root.iter(Some("Trade"))), ["Trade", "Trade"]);
        assert_eq!(
            tags(root.iter(Some("FlexQueryResponse"))),
            ["FlexQueryResponse"]
        );

        // find and findall look at children only.
        assert!(root.find("Trades").is_none());
        assert_eq!(root.findall("FlexStatements").len(), 1);
        let trades = root
            .find("FlexStatements")
            .and_then(|e| e.find("FlexStatement"))
            .and_then(|e| e.find("Trades"))
            .unwrap();
        // The comment drops out and the text around it joins.
        assert_eq!(trades.text.as_deref(), Some("\n\n"));
        assert_eq!(trades.findall("Trade").len(), 2);
        assert!(
            trades
                .children
                .iter()
                .all(|c| c.tail.as_deref() == Some("\n"))
        );
        assert_eq!(trades.tail.as_deref(), Some("\n"));
        assert_eq!(trades.children[0].text, None);

        // Namespaces, a processing instruction, a document type and a BOM.
        let r = tree(r#"<r xmlns="u"><c a="1" xml:lang="en"/>x<?pi y?>z</r>"#);
        assert_eq!(r.tag, "{u}r");
        assert_eq!(r.children[0].tag, "{u}c");
        let keys: Vec<_> = r.children[0].attrib.keys().collect();
        assert_eq!(keys, ["a", "{http://www.w3.org/XML/1998/namespace}lang"]);
        assert_eq!(r.children[0].tail.as_deref(), Some("xz"));
        let r = tree(r#"<!DOCTYPE r [<!ENTITY e "v">]><r a="&e;">&e;</r>"#);
        assert_eq!(
            (r.attrib["a"].as_str(), r.text.as_deref()),
            ("v", Some("v"))
        );
        assert_eq!(tree("\u{feff}<r/>").tag, "r");
    }

    #[test]
    fn invalid_xml_is_a_flex_error() {
        let bad = FlexReport {
            data: b"<a>".to_vec(),
        };
        assert!(matches!(bad.root(), Err(Error::Flex(_))));
        assert!(matches!(bad.topics(), Err(Error::Flex(_))));
        assert!(matches!(bad.extract("a", true), Err(Error::Flex(_))));
        assert!(matches!(FlexReport::default().root(), Err(Error::Flex(_))));

        let path = temp("bad.xml");
        bad.save(&path).unwrap();
        let loaded = FlexReport::load(&path);
        std::fs::remove_file(&path).unwrap();
        assert!(matches!(loaded, Err(Error::Flex(_))));
        assert!(matches!(
            FlexReport::load(temp("missing.xml")),
            Err(Error::Io(_))
        ));
    }

    // Each expectation is `urlparse`'s scheme and netloc test in ib_async's
    // `get_url`, run on Python 3.14.
    #[test]
    fn get_url_validation() {
        for (url, valid) in [
            (FLEXREPORT_URL, true),
            ("", false),
            ("ndcdyn.interactivebrokers.com/x", false),
            ("https://", false),
            ("http://[::1", false),
            ("http://[::1]:8080/x", true),
            ("http://[fe80::1%25eth0]/", true),
            ("http://[1.2.3.4]/", false),
            ("http://[v1.x]/", true),
            ("http://[vz.x]/", false),
            ("http://a[::1]/", false),
            ("http://[::1]x/", false),
            ("http://]h/", false),
            ("http://[::1]:/", true),
            ("file:///tmp/x", false),
            ("localhost:8080/path", false),
            (" \t https://h/", true),
            ("ht\ntp://h/", true),
            ("1http://h/", false),
            ("h_t://h/", false),
            ("HTTP://h?x", true),
            ("https://h#f", true),
            ("https://u@h:1", true),
        ] {
            assert_eq!(is_valid_url(url), valid, "{url:?}");
        }
        match std::env::var("IB_FLEXREPORT_URL") {
            Err(VarError::NotPresent) => {
                assert_eq!(FlexReport::get_url().unwrap(), FLEXREPORT_URL);
            }
            Ok(url) if !is_valid_url(&url) => {
                let err = FlexReport::get_url().unwrap_err();
                assert_eq!(
                    err.to_string(),
                    "Invalid URL, please check that env variable IB_FLEXREPORT_URL is set correctly."
                );
            }
            _ => {}
        }
    }

    #[test]
    fn download_answers_read_as_ib_async_reads_them() {
        let ok = tree(
            "<FlexStatementResponse><Status>Success</Status>\
             <ReferenceCode>123</ReferenceCode><Url>https://h/Get</Url></FlexStatementResponse>",
        );
        assert_eq!(
            reference(&ok).unwrap(),
            ("123".to_owned(), "https://h/Get".to_owned())
        );
        let no_url = tree("<r><Status>Success</Status><ReferenceCode>1</ReferenceCode></r>");
        assert!(matches!(reference(&no_url), Err(Error::Value(_))));
        let refused = tree(
            "<r><Status>Fail</Status><ErrorCode>1012</ErrorCode>\
             <ErrorMessage>Token has expired.</ErrorMessage></r>",
        );
        assert_eq!(
            reference(&refused).unwrap_err().to_string(),
            "1012: Token has expired."
        );
        let bare = tree("<r><ErrorCode/></r>");
        assert_eq!(reference(&bare).unwrap_err().to_string(), "None: ");

        let working =
            tree("<r><code>Statement generation in progress. Please try again shortly.</code></r>");
        assert!(in_progress(&working).unwrap());
        let failed = tree("<r><code>Statement could not be generated.</code></r>");
        assert_eq!(
            in_progress(&failed).unwrap_err().to_string(),
            "Statement could not be generated."
        );
        assert!(!in_progress(&tree(REPORT)).unwrap());
        assert!(matches!(in_progress(&tree("<r/>")), Err(Error::Value(_))));
    }
}
