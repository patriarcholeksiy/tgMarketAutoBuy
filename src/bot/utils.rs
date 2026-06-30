use crate::accounts::pool::ViewersReport;

pub fn format_proxy_string(raw_input: &str) -> Option<String> {
    let input = raw_input.trim();

    if input.starts_with("http://") || input.starts_with("https://") || input.starts_with("socks5://") {
        return Some(input.to_string())
    }

    let parts: Vec<&str> = input.split(':').collect();

    match parts.as_slice() {
        [ip, port, user, pass] => Some(format!("http://{user}:{pass}@{ip}:{port}")),
        [ip, port] => Some(format!("http://{ip}:{port}")),
        _ => None,
    }
}

pub fn generate_report_message(report: ViewersReport) -> String {
    let mut msg = String::new();

    if report.added.is_empty() && report.failed.is_empty() {
        msg.push_str("ℹ️ There are no new accounts to add.");
    } else {
        if !report.added.is_empty() {
            msg.push_str(&format!("✅ Successfully added: {}\n", report.added.len()));

            for (id, phone) in report.added {
                msg.push_str(&format!("• ID {id} with phone {phone:?}\n"));
            }
        }

        if !report.failed.is_empty() {
            msg.push_str(&format!("❌ Adding error: {}\n", report.failed.len()));
            for (id, phone, reason) in report.failed {
                msg.push_str(&format!("• ID {id} with phone {phone:?}: {reason}\n"));
            }
        }
    }

    msg
}