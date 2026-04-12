
pub fn elevated() -> bool {
    #[cfg(windows)]
    {
        is_elevated::is_elevated()
    }
    #[cfg(unix)]
    {
        nix::unistd::geteuid().is_root()
    }
}
pub fn get_user_id() -> anyhow::Result<String> {
    #[cfg(windows)]{
        use windows::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
        use windows::Win32::Security::{GetTokenInformation, TokenUser, TOKEN_QUERY, TOKEN_USER};
        use windows::Win32::Security::Authorization::ConvertSidToStringSidW;
        use windows::Win32::Foundation::{HANDLE, LocalFree, CloseHandle,HLOCAL};
        use windows::core::PWSTR;

        unsafe {
            let mut token = HANDLE::default();

            OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token)?;

            let mut len = 0u32;
            let _ = GetTokenInformation(token, TokenUser, None, 0, &mut len);

            let mut buf = vec![0u8; len as usize];
            GetTokenInformation(token, TokenUser, Some(buf.as_mut_ptr() as _), len, &mut len)?;

            let token_user = &*(buf.as_ptr() as *const TOKEN_USER);
            let mut sid_str = PWSTR::null();
          ConvertSidToStringSidW(token_user.User.Sid, &mut sid_str)?;

            let result = sid_str.to_string()?;
            LocalFree(Some(HLOCAL(sid_str.0 as _)));
            CloseHandle(token)?;

            Ok(result)
        }
    }
    #[cfg(unix)]{
        use nix::unistd::geteuid;
        Ok(geteuid().to_string())
    }

}