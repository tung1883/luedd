//! Assemble a PDF from page images (Lüdd-Docs). JPEG bytes are embedded as-is
//! (`DCTDecode`), so there is no re-encode and no extra dependency.

use anyhow::{bail, Result};

/// Page width in PDF points (A4); height follows the image aspect ratio.
const PAGE_W: f64 = 595.28;

struct Jpeg {
    w: u32,
    h: u32,
    components: u8,
    bits: u8,
}

/// Read size/components from the first SOFn marker.
fn jpeg_info(b: &[u8]) -> Result<Jpeg> {
    if b.len() < 4 || b[0] != 0xFF || b[1] != 0xD8 {
        bail!("not a JPEG");
    }
    let mut i = 2;
    while i + 4 <= b.len() {
        if b[i] != 0xFF {
            i += 1;
            continue;
        }
        let m = b[i + 1];
        if m == 0xFF {
            i += 1;
            continue;
        }
        if m == 0xD8 || m == 0x01 || (0xD0..=0xD7).contains(&m) {
            i += 2;
            continue;
        }
        let len = u16::from_be_bytes([b[i + 2], b[i + 3]]) as usize;
        // SOF0..SOF15 except DHT(C4), JPG(C8), DAC(CC)
        if (0xC0..=0xCF).contains(&m) && !matches!(m, 0xC4 | 0xC8 | 0xCC) {
            if i + 10 > b.len() {
                break;
            }
            let bits = b[i + 4];
            let h = u16::from_be_bytes([b[i + 5], b[i + 6]]) as u32;
            let w = u16::from_be_bytes([b[i + 7], b[i + 8]]) as u32;
            let components = b[i + 9];
            if w == 0 || h == 0 {
                bail!("empty JPEG");
            }
            return Ok(Jpeg { w, h, components, bits });
        }
        i += 2 + len;
    }
    bail!("JPEG has no frame header")
}

/// `<FEFF....>` UTF-16BE hex string for the document title.
fn pdf_text_string(s: &str) -> String {
    let mut out = String::from("<FEFF");
    for u in s.encode_utf16() {
        out.push_str(&format!("{u:04X}"));
    }
    out.push('>');
    out
}

pub fn jpegs_to_pdf(pages: &[Vec<u8>], title: &str) -> Result<Vec<u8>> {
    if pages.is_empty() {
        bail!("no pages");
    }
    let infos = pages.iter().map(|p| jpeg_info(p)).collect::<Result<Vec<_>>>()?;

    // object numbers: 1 catalog, 2 pages, 3 info, then per page: page, content, image
    let n = pages.len();
    let page_obj = |i: usize| 4 + i * 3;
    let mut out: Vec<u8> = Vec::new();
    let mut offsets: Vec<usize> = vec![0; 4 + n * 3];
    out.extend_from_slice(b"%PDF-1.4\n%\xE2\xE3\xCF\xD3\n");

    let mut obj = |out: &mut Vec<u8>, id: usize, body: &[u8]| {
        offsets[id] = out.len();
        out.extend_from_slice(format!("{id} 0 obj\n").as_bytes());
        out.extend_from_slice(body);
        out.extend_from_slice(b"\nendobj\n");
    };

    obj(&mut out, 1, b"<< /Type /Catalog /Pages 2 0 R >>");
    let kids: String = (0..n).map(|i| format!("{} 0 R", page_obj(i))).collect::<Vec<_>>().join(" ");
    obj(&mut out, 2, format!("<< /Type /Pages /Kids [{kids}] /Count {n} >>").as_bytes());
    obj(&mut out, 3, format!("<< /Title {} /Producer (Ludd) >>", pdf_text_string(title)).as_bytes());

    for (i, (data, info)) in pages.iter().zip(&infos).enumerate() {
        let pid = page_obj(i);
        let ph = PAGE_W * info.h as f64 / info.w as f64;
        obj(
            &mut out,
            pid,
            format!(
                "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {PAGE_W:.2} {ph:.2}] /Resources << /XObject << /Im {} 0 R >> >> /Contents {} 0 R >>",
                pid + 2,
                pid + 1
            )
            .as_bytes(),
        );
        let content = format!("q {PAGE_W:.2} 0 0 {ph:.2} 0 0 cm /Im Do Q");
        obj(
            &mut out,
            pid + 1,
            format!("<< /Length {} >>\nstream\n{content}\nendstream", content.len()).as_bytes(),
        );
        let cs = match info.components {
            1 => "/DeviceGray",
            4 => "/DeviceCMYK",
            _ => "/DeviceRGB",
        };
        // Adobe CMYK JPEGs are stored inverted
        let decode = if info.components == 4 { " /Decode [1 0 1 0 1 0 1 0]" } else { "" };
        let mut body = format!(
            "<< /Type /XObject /Subtype /Image /Width {} /Height {} /ColorSpace {cs} /BitsPerComponent {} /Filter /DCTDecode{decode} /Length {} >>\nstream\n",
            info.w,
            info.h,
            info.bits,
            data.len()
        )
        .into_bytes();
        body.extend_from_slice(data);
        body.extend_from_slice(b"\nendstream");
        obj(&mut out, pid + 2, &body);
    }

    let xref = out.len();
    out.extend_from_slice(format!("xref\n0 {}\n0000000000 65535 f \n", offsets.len()).as_bytes());
    for off in &offsets[1..] {
        out.extend_from_slice(format!("{off:010} 00000 n \n").as_bytes());
    }
    out.extend_from_slice(
        format!("trailer\n<< /Size {} /Root 1 0 R /Info 3 0 R >>\nstartxref\n{xref}\n%%EOF\n", offsets.len()).as_bytes(),
    );
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smallest header-only JPEG: SOI + SOF0 (w x h, 3 components) + EOI.
    fn fake_jpeg(w: u16, h: u16) -> Vec<u8> {
        let mut v = vec![0xFF, 0xD8, 0xFF, 0xC0, 0x00, 0x11, 8];
        v.extend_from_slice(&h.to_be_bytes());
        v.extend_from_slice(&w.to_be_bytes());
        v.extend_from_slice(&[3, 1, 0x22, 0, 2, 0x11, 1, 3, 0x11, 1]);
        v.extend_from_slice(&[0xFF, 0xD9]);
        v
    }

    #[test]
    fn reads_jpeg_size() {
        let j = jpeg_info(&fake_jpeg(2400, 3200)).unwrap();
        assert_eq!((j.w, j.h, j.components, j.bits), (2400, 3200, 3, 8));
    }

    #[test]
    fn rejects_non_jpeg() {
        assert!(jpeg_info(b"\x89PNG\r\n").is_err());
        assert!(jpegs_to_pdf(&[], "x").is_err());
    }

    #[test]
    fn builds_pdf_with_every_page() {
        let pdf = jpegs_to_pdf(&[fake_jpeg(100, 200), fake_jpeg(300, 100)], "Doc \u{e4}").unwrap();
        let s = String::from_utf8_lossy(&pdf);
        assert!(s.starts_with("%PDF-1.4"));
        assert!(s.contains("/Count 2"));
        assert_eq!(s.matches("/DCTDecode").count(), 2);
        assert!(s.trim_end().ends_with("%%EOF"));
        // xref offsets point at "N 0 obj"
        let xr = s.rfind("startxref\n").unwrap();
        let start: usize = s[xr + 10..].lines().next().unwrap().parse().unwrap();
        assert!(pdf[start..].starts_with(b"xref"));
    }
}
