#[cfg(test)]
mod tests {
    use crate::data::io::format_file_size_pub;

    #[test]
    fn test_format_file_size() {
        assert_eq!(format_file_size_pub(0), "0 B");
        assert_eq!(format_file_size_pub(512), "512 B");
        assert_eq!(format_file_size_pub(1024), "1.0 KB");
        assert_eq!(format_file_size_pub(1536), "1.5 KB");
        assert_eq!(format_file_size_pub(1024 * 1024), "1.0 MB");
        assert_eq!(format_file_size_pub(1024 * 1024 * 1024), "1.0 GB");
    }
}
