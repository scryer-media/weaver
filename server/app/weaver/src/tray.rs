#![cfg_attr(windows, windows_subsystem = "windows")]

#[cfg(not(windows))]
fn main() {
    eprintln!("weaver-tray is only supported on Windows");
    std::process::exit(1);
}

#[cfg(windows)]
fn main() {
    if let Err(error) = windows::run() {
        windows::show_error("Weaver", &error);
        std::process::exit(1);
    }
}

#[cfg(windows)]
#[path = "tray_ipc.rs"]
mod tray_ipc;

#[cfg(windows)]
mod windows {
    use std::ffi::c_void;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command};
    use std::ptr;
    use std::thread;
    use std::time::{Duration, Instant};

    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_ALREADY_EXISTS, GetLastError, HANDLE, HWND, LPARAM, LRESULT, POINT,
        WPARAM,
    };
    use windows_sys::Win32::System::LibraryLoader::GetModuleHandleW;
    use windows_sys::Win32::System::Registry::{
        HKEY, HKEY_CURRENT_USER, KEY_QUERY_VALUE, KEY_SET_VALUE, REG_SZ, RegCloseKey,
        RegCreateKeyExW, RegDeleteValueW, RegOpenKeyExW, RegQueryValueExW, RegSetValueExW,
    };
    use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CreateMutexW};
    use windows_sys::Win32::UI::Shell::{
        NIF_ICON, NIF_MESSAGE, NIF_TIP, NIM_ADD, NIM_DELETE, NOTIFYICONDATAW, Shell_NotifyIconW,
        ShellExecuteW,
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        AppendMenuW, CreatePopupMenu, CreateWindowExW, DefWindowProcW, DestroyMenu, DestroyWindow,
        DispatchMessageW, FindWindowW, GWLP_USERDATA, GetCursorPos, GetMessageW, GetWindowLongPtrW,
        HMENU, LoadIconW, MB_ICONERROR, MB_OK, MF_CHECKED, MF_SEPARATOR, MF_STRING, MF_UNCHECKED,
        MSG, MessageBoxW, PostMessageW, PostQuitMessage, RegisterClassW, SW_SHOWNORMAL,
        SetForegroundWindow, SetWindowLongPtrW, TPM_RETURNCMD, TPM_RIGHTBUTTON, TrackPopupMenu,
        TranslateMessage, WM_DESTROY, WM_LBUTTONUP, WM_RBUTTONUP, WNDCLASSW,
    };

    // The window class and message ids are shared with weaver.exe, which posts
    // the restart message to this window.
    use super::tray_ipc::{
        CLASS_NAME, OPEN_WINDOW_MESSAGE, RESTART_MESSAGE, SHUTDOWN_MESSAGE, TRAY_CALLBACK_MESSAGE,
    };

    const DEFAULT_PORT: u16 = 9090;
    const MUTEX_NAMESPACE: &str = "Global\\ScryerMedia.Weaver.Desktop.v1.Tray.";
    const RUN_KEY: &str = "Software\\Microsoft\\Windows\\CurrentVersion\\Run";
    const RUN_VALUE: &str = "ScryerMedia.Weaver";
    const WEAVER_ICON_RESOURCE_ID: usize = 1;
    /// How long the tray waits for a server that asked to be restarted to
    /// actually exit before it stops waiting and kills the child.
    const SERVER_EXIT_TIMEOUT: Duration = Duration::from_secs(30);

    const MENU_OPEN: u32 = 1;
    const MENU_START: u32 = 2;
    const MENU_STOP: u32 = 3;
    const MENU_RESTART: u32 = 4;
    const MENU_OPEN_LOGS: u32 = 5;
    const MENU_TOGGLE_STARTUP: u32 = 6;
    const MENU_EXIT: u32 = 7;

    enum LaunchMode {
        Interactive,
        Login,
        Shutdown,
        UnregisterStartup,
    }

    pub(super) fn run() -> Result<(), String> {
        match launch_mode()? {
            LaunchMode::UnregisterStartup => return unregister_startup(),
            LaunchMode::Shutdown => return shutdown_existing_instance(),
            LaunchMode::Interactive | LaunchMode::Login => {}
        }

        let instance = InstanceGuard::acquire()?;
        if !instance.is_primary() {
            // FindWindowW is session-local. A same-session invocation opens the existing
            // window; a second session for the same user simply leaves the owner alone.
            let _ = signal_existing_instance(OPEN_WINDOW_MESSAGE);
            return Ok(());
        }

        let profile_dir = desktop_profile_dir()?;
        std::fs::create_dir_all(profile_dir.join("logs")).map_err(|error| {
            format!(
                "failed to create Weaver desktop profile at {}: {error}",
                profile_dir.display()
            )
        })?;

        let class_name = wide(CLASS_NAME);
        let window_class = WNDCLASSW {
            lpfnWndProc: Some(window_proc),
            lpszClassName: class_name.as_ptr(),
            ..Default::default()
        };
        // SAFETY: The class name and callback remain valid for process lifetime.
        if unsafe { RegisterClassW(&window_class) } == 0 {
            return Err(format!(
                "failed to register Weaver tray window class: {}",
                std::io::Error::last_os_error()
            ));
        }

        // SAFETY: The registered class name is valid and this creates an invisible top-level window.
        let window = unsafe {
            CreateWindowExW(
                0,
                class_name.as_ptr(),
                ptr::null(),
                0,
                0,
                0,
                0,
                0,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null(),
            )
        };
        if window.is_null() {
            return Err(format!(
                "failed to create Weaver tray window: {}",
                std::io::Error::last_os_error()
            ));
        }

        let mode = launch_mode()?;
        let state = Box::new(TrayState::new(
            profile_dir,
            matches!(mode, LaunchMode::Login),
        ));
        let state = Box::into_raw(state);
        // SAFETY: The Box allocation remains alive until after the message loop exits.
        unsafe { SetWindowLongPtrW(window, GWLP_USERDATA, state.cast::<c_void>() as isize) };

        // SAFETY: State is initialized and uniquely owned by this UI thread.
        let startup_result = unsafe { (&mut *state).initialize(window) };
        if let Err(error) = startup_result {
            // SAFETY: The window is ours and the Box was allocated above.
            unsafe {
                DestroyWindow(window);
                drop(Box::from_raw(state));
            }
            return Err(error);
        }

        let mut message = MSG::default();
        loop {
            // SAFETY: `message` is valid writable storage for the duration of the call.
            let result = unsafe { GetMessageW(&mut message, ptr::null_mut(), 0, 0) };
            if result == -1 {
                // SAFETY: The Box was allocated above and is no longer accessed after this drop.
                unsafe { drop(Box::from_raw(state)) };
                return Err(format!(
                    "failed to receive Weaver tray message: {}",
                    std::io::Error::last_os_error()
                ));
            }
            if result == 0 {
                break;
            }
            // SAFETY: The message was populated by GetMessageW.
            unsafe {
                TranslateMessage(&message);
                DispatchMessageW(&message);
            }
        }

        // SAFETY: The state is no longer reachable once the window was destroyed.
        unsafe { drop(Box::from_raw(state)) };
        drop(instance);
        Ok(())
    }

    fn launch_mode() -> Result<LaunchMode, String> {
        let mut args = std::env::args_os();
        let _program = args.next();
        match args.next() {
            None => Ok(LaunchMode::Interactive),
            Some(value) if value == "--login-start" => Ok(LaunchMode::Login),
            Some(value) if value == "--shutdown" => Ok(LaunchMode::Shutdown),
            Some(value) if value == "--unregister-startup" => Ok(LaunchMode::UnregisterStartup),
            Some(value) if value == "--version" || value == "-V" => {
                println!("{}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            Some(value) => Err(format!(
                "unrecognized weaver-tray argument: {}",
                value.to_string_lossy()
            )),
        }
    }

    fn desktop_profile_dir() -> Result<PathBuf, String> {
        let local_app_data = std::env::var_os("LOCALAPPDATA").ok_or_else(|| {
            "LOCALAPPDATA is not set; cannot locate Weaver desktop data".to_string()
        })?;
        Ok(desktop_profile_dir_from(Path::new(&local_app_data)))
    }

    fn desktop_profile_dir_from(local_app_data: &Path) -> PathBuf {
        local_app_data.join("ScryerMedia").join("Weaver")
    }

    fn tray_mutex_name() -> Result<String, String> {
        let username = std::env::var("USERNAME").map_err(|_| {
            "USERNAME is not set; cannot scope the Weaver tray instance".to_string()
        })?;
        let domain = std::env::var("USERDOMAIN").unwrap_or_default();
        Ok(tray_mutex_name_for_user(&domain, &username))
    }

    fn tray_mutex_name_for_user(domain: &str, username: &str) -> String {
        let identity = if domain.is_empty() {
            username.to_string()
        } else {
            format!("{domain}\\{username}")
        };
        let encoded = identity
            .encode_utf16()
            .map(|unit| format!("{unit:04X}"))
            .collect::<String>();
        format!("{MUTEX_NAMESPACE}{encoded}")
    }

    struct InstanceGuard(HANDLE, bool);

    impl InstanceGuard {
        fn acquire() -> Result<Self, String> {
            let name = wide(&tray_mutex_name()?);
            // SAFETY: `name` is nul-terminated and remains live for this call.
            let handle = unsafe { CreateMutexW(ptr::null(), 0, name.as_ptr()) };
            if handle.is_null() {
                return Err(format!(
                    "failed to create Weaver tray instance mutex: {}",
                    std::io::Error::last_os_error()
                ));
            }
            // SAFETY: GetLastError reads the result associated with CreateMutexW above.
            let is_primary = unsafe { GetLastError() } != ERROR_ALREADY_EXISTS;
            Ok(Self(handle, is_primary))
        }

        fn is_primary(&self) -> bool {
            self.1
        }
    }

    impl Drop for InstanceGuard {
        fn drop(&mut self) {
            // SAFETY: This guard owns the mutex handle returned by CreateMutexW.
            unsafe { CloseHandle(self.0) };
        }
    }

    fn signal_existing_instance(message: u32) -> Result<(), String> {
        let class_name = wide(CLASS_NAME);
        for _ in 0..40 {
            // SAFETY: The class name is a valid nul-terminated UTF-16 string.
            let window = unsafe { FindWindowW(class_name.as_ptr(), ptr::null()) };
            if !window.is_null() {
                // SAFETY: The target is a same-user Weaver tray window identified by its private class.
                if unsafe { PostMessageW(window, message, 0, 0) } == 0 {
                    return Err(format!(
                        "failed to signal existing Weaver tray instance: {}",
                        std::io::Error::last_os_error()
                    ));
                }
                return Ok(());
            }
            thread::sleep(Duration::from_millis(50));
        }
        Err("another Weaver tray instance is starting but did not create its window".to_string())
    }

    fn shutdown_existing_instance() -> Result<(), String> {
        let instance = InstanceGuard::acquire()?;
        if instance.is_primary() {
            return Ok(());
        }
        signal_existing_instance(SHUTDOWN_MESSAGE)?;

        let class_name = wide(CLASS_NAME);
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            // SAFETY: The class name is a valid nul-terminated UTF-16 string.
            if unsafe { FindWindowW(class_name.as_ptr(), ptr::null()) }.is_null() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err("timed out waiting for the existing Weaver tray to stop".to_string())
    }

    struct TrayState {
        profile_dir: PathBuf,
        login_start: bool,
        server: Option<Child>,
        icon_added: bool,
    }

    impl TrayState {
        fn new(profile_dir: PathBuf, login_start: bool) -> Self {
            Self {
                profile_dir,
                login_start,
                server: None,
                icon_added: false,
            }
        }

        unsafe fn initialize(&mut self, window: HWND) -> Result<(), String> {
            // SAFETY: The window is live for the duration of tray initialization.
            unsafe { self.add_icon(window)? };
            if self.login_start {
                self.start_server()?;
            } else {
                self.enable_startup()?;
                self.open_weaver()?;
            }
            Ok(())
        }

        unsafe fn add_icon(&mut self, window: HWND) -> Result<(), String> {
            // SAFETY: Resource ID 1 is the application-owned multi-resolution Weaver icon.
            let icon = unsafe {
                LoadIconW(
                    GetModuleHandleW(ptr::null()),
                    WEAVER_ICON_RESOURCE_ID as *const u16,
                )
            };
            if icon.is_null() {
                return Err(format!(
                    "failed to load Weaver tray icon: {}",
                    std::io::Error::last_os_error()
                ));
            }
            let mut data = NOTIFYICONDATAW {
                cbSize: std::mem::size_of::<NOTIFYICONDATAW>() as u32,
                hWnd: window,
                uID: 1,
                uFlags: NIF_MESSAGE | NIF_ICON | NIF_TIP,
                uCallbackMessage: TRAY_CALLBACK_MESSAGE,
                hIcon: icon,
                ..Default::default()
            };
            write_wide_buffer(&mut data.szTip, "Weaver");
            // SAFETY: `data` is initialized and remains live through the system call.
            if unsafe { Shell_NotifyIconW(NIM_ADD, &data) } == 0 {
                return Err(format!(
                    "failed to add Weaver tray icon: {}",
                    std::io::Error::last_os_error()
                ));
            }
            self.icon_added = true;
            Ok(())
        }

        unsafe fn remove_icon(&mut self, window: HWND) {
            if !self.icon_added {
                return;
            }
            let data = NOTIFYICONDATAW {
                cbSize: std::mem::size_of::<NOTIFYICONDATAW>() as u32,
                hWnd: window,
                uID: 1,
                ..Default::default()
            };
            // SAFETY: The notification data identifies the icon added by this process.
            unsafe { Shell_NotifyIconW(NIM_DELETE, &data) };
            self.icon_added = false;
        }

        fn open_weaver(&mut self) -> Result<(), String> {
            self.start_server()?;
            if !wait_for_server(DEFAULT_PORT, Duration::from_secs(30)) {
                return Err(
                    "timed out waiting for Weaver to become ready at http://127.0.0.1:9090".into(),
                );
            }
            open_target("http://127.0.0.1:9090/")
        }

        fn start_server(&mut self) -> Result<(), String> {
            if server_ready(DEFAULT_PORT) {
                return Ok(());
            }
            if let Some(child) = self.server.as_mut() {
                match child.try_wait() {
                    Ok(None) => return Ok(()),
                    Ok(Some(_)) => self.server = None,
                    Err(error) => {
                        return Err(format!("failed to check Weaver server status: {error}"));
                    }
                }
            }

            let tray_exe = std::env::current_exe()
                .map_err(|error| format!("failed to resolve weaver-tray.exe path: {error}"))?;
            let weaver_exe = tray_exe.with_file_name("weaver.exe");
            if !weaver_exe.is_file() {
                return Err(format!(
                    "weaver.exe was not found beside weaver-tray.exe at {}",
                    weaver_exe.display()
                ));
            }
            let log_file = self.profile_dir.join("logs").join("weaver.log");
            let child = Command::new(&weaver_exe)
                .arg("--config")
                .arg(&self.profile_dir)
                .arg("--log-file")
                .arg(&log_file)
                .args(["serve", "--port", &DEFAULT_PORT.to_string()])
                .creation_flags(CREATE_NO_WINDOW)
                .spawn()
                .map_err(|error| {
                    format!(
                        "failed to start Weaver from {}: {error}",
                        weaver_exe.display()
                    )
                })?;
            self.server = Some(child);
            Ok(())
        }

        fn stop_server(&mut self) -> Result<(), String> {
            let Some(mut child) = self.server.take() else {
                return Ok(());
            };
            if child
                .try_wait()
                .map_err(|error| format!("failed to check Weaver server status: {error}"))?
                .is_none()
            {
                child
                    .kill()
                    .map_err(|error| format!("failed to stop Weaver server: {error}"))?;
                child
                    .wait()
                    .map_err(|error| format!("failed to wait for Weaver server exit: {error}"))?;
            }
            Ok(())
        }

        fn restart_server(&mut self) -> Result<(), String> {
            self.stop_server()?;
            self.start_server()?;
            if wait_for_server(DEFAULT_PORT, Duration::from_secs(30)) {
                Ok(())
            } else {
                Err("timed out waiting for Weaver after restart".to_string())
            }
        }

        /// Restart a server that asked to be restarted from its own UI.
        ///
        /// Unlike the menu path, the server is already tearing itself down, so
        /// the old process must be gone before the replacement starts:
        /// `start_server` returns early while the port still answers, so
        /// starting without waiting would silently leave the user with no
        /// server at all.
        fn restart_requested_by_server(&mut self) -> Result<(), String> {
            self.wait_for_server_exit(SERVER_EXIT_TIMEOUT);
            self.start_server()?;
            if wait_for_server(DEFAULT_PORT, Duration::from_secs(30)) {
                Ok(())
            } else {
                Err("timed out waiting for Weaver after restart".to_string())
            }
        }

        /// Wait for the running server to disappear, bounded so a process that
        /// never exits cannot strand the tray. The owned child is the reliable
        /// signal; when the tray does not own one, the port answering is the
        /// only evidence left. Falls back to the kill path on timeout.
        fn wait_for_server_exit(&mut self, timeout: Duration) {
            let deadline = Instant::now() + timeout;
            while Instant::now() < deadline {
                match self.server.as_mut() {
                    Some(child) => match child.try_wait() {
                        Ok(Some(_)) => {
                            self.server = None;
                            return;
                        }
                        Ok(None) => {}
                        Err(_) => break,
                    },
                    None => {
                        if !server_ready(DEFAULT_PORT) {
                            return;
                        }
                    }
                }
                thread::sleep(Duration::from_millis(250));
            }
            let _ = self.stop_server();
        }

        fn show_menu(&mut self, window: HWND) -> Result<(), String> {
            // SAFETY: CreatePopupMenu creates a menu owned by this function until DestroyMenu.
            let menu = unsafe { CreatePopupMenu() };
            if menu.is_null() {
                return Err(format!(
                    "failed to create Weaver tray menu: {}",
                    std::io::Error::last_os_error()
                ));
            }

            let result = (|| {
                append_menu(menu, MENU_OPEN, "Open Weaver", MF_STRING)?;
                append_menu(menu, MENU_START, "Start Weaver", MF_STRING)?;
                append_menu(menu, MENU_STOP, "Stop Weaver", MF_STRING)?;
                append_menu(menu, MENU_RESTART, "Restart Weaver", MF_STRING)?;
                // SAFETY: A separator does not use its string argument.
                unsafe { AppendMenuW(menu, MF_SEPARATOR, 0, ptr::null()) };
                append_menu(menu, MENU_OPEN_LOGS, "Open Logs", MF_STRING)?;
                let startup_flags = if startup_enabled()? {
                    MF_STRING | MF_CHECKED
                } else {
                    MF_STRING | MF_UNCHECKED
                };
                append_menu(menu, MENU_TOGGLE_STARTUP, "Start at sign-in", startup_flags)?;
                // SAFETY: A separator does not use its string argument.
                unsafe { AppendMenuW(menu, MF_SEPARATOR, 0, ptr::null()) };
                append_menu(menu, MENU_EXIT, "Exit", MF_STRING)?;

                let mut point = POINT::default();
                // SAFETY: `point` is writable storage for the system call.
                if unsafe { GetCursorPos(&mut point) } == 0 {
                    return Err(format!(
                        "failed to get cursor position for Weaver tray menu: {}",
                        std::io::Error::last_os_error()
                    ));
                }
                // SAFETY: The tray window is owned by this process and the menu remains live.
                unsafe { SetForegroundWindow(window) };
                // SAFETY: The menu and owner window remain valid through the call.
                let command = unsafe {
                    TrackPopupMenu(
                        menu,
                        TPM_RETURNCMD | TPM_RIGHTBUTTON,
                        point.x,
                        point.y,
                        0,
                        window,
                        ptr::null(),
                    )
                };
                self.handle_menu_command(window, command as u32)
            })();

            // SAFETY: This function owns the menu created above.
            unsafe { DestroyMenu(menu) };
            result
        }

        fn handle_menu_command(&mut self, window: HWND, command: u32) -> Result<(), String> {
            match command {
                0 => Ok(()),
                MENU_OPEN => self.open_weaver(),
                MENU_START => self.start_server(),
                MENU_STOP => self.stop_server(),
                MENU_RESTART => self.restart_server(),
                MENU_OPEN_LOGS => open_target(&self.profile_dir.join("logs").to_string_lossy()),
                MENU_TOGGLE_STARTUP => {
                    if startup_enabled()? {
                        unregister_startup()
                    } else {
                        self.enable_startup()
                    }
                }
                MENU_EXIT => {
                    // SAFETY: `window` is the live tray window for this state.
                    unsafe { DestroyWindow(window) };
                    Ok(())
                }
                _ => Ok(()),
            }
        }

        fn enable_startup(&self) -> Result<(), String> {
            let executable = std::env::current_exe()
                .map_err(|error| format!("failed to resolve weaver-tray.exe path: {error}"))?;
            register_startup(&executable)
        }
    }

    unsafe extern "system" fn window_proc(
        window: HWND,
        message: u32,
        _wparam: WPARAM,
        lparam: LPARAM,
    ) -> LRESULT {
        // SAFETY: The pointer was installed from a live Box immediately after window creation.
        let state = unsafe { GetWindowLongPtrW(window, GWLP_USERDATA) as *mut TrayState };
        if !state.is_null() {
            // SAFETY: The message loop serializes access to the state on this UI thread.
            let state = unsafe { &mut *state };
            let result = match message {
                TRAY_CALLBACK_MESSAGE if lparam as u32 == WM_LBUTTONUP => state.open_weaver(),
                TRAY_CALLBACK_MESSAGE if lparam as u32 == WM_RBUTTONUP => state.show_menu(window),
                OPEN_WINDOW_MESSAGE => state.open_weaver(),
                RESTART_MESSAGE => state.restart_requested_by_server(),
                SHUTDOWN_MESSAGE => {
                    // SAFETY: This is the live window associated with the tray state.
                    unsafe { DestroyWindow(window) };
                    Ok(())
                }
                WM_DESTROY => {
                    // SAFETY: The icon belongs to this window and is being removed during teardown.
                    unsafe { state.remove_icon(window) };
                    let _ = state.stop_server();
                    // SAFETY: Ends the GetMessageW loop in this process.
                    unsafe { PostQuitMessage(0) };
                    return 0;
                }
                _ => {
                    // SAFETY: Default processing is required for messages the tray does not own.
                    return unsafe { DefWindowProcW(window, message, _wparam, lparam) };
                }
            };
            if let Err(error) = result {
                show_error("Weaver", &error);
            }
            return 0;
        }

        // SAFETY: The window has not yet had its state attached, so default handling is correct.
        unsafe { DefWindowProcW(window, message, _wparam, lparam) }
    }

    fn append_menu(menu: HMENU, id: u32, label: &str, flags: u32) -> Result<(), String> {
        let label = wide(label);
        // SAFETY: The menu is owned by the caller and the UTF-16 label remains live for the call.
        if unsafe { AppendMenuW(menu, flags, id as usize, label.as_ptr()) } == 0 {
            return Err(format!(
                "failed to add Weaver tray menu item: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }

    fn wait_for_server(port: u16, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if server_ready(port) {
                return true;
            }
            thread::sleep(Duration::from_millis(250));
        }
        false
    }

    fn server_ready(port: u16) -> bool {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(250))
        else {
            return false;
        };
        let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
        let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
        if stream
            .write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .is_err()
        {
            return false;
        }
        let mut response = [0u8; 128];
        let Ok(read) = stream.read(&mut response) else {
            return false;
        };
        response[..read].starts_with(b"HTTP/1.1 200")
            || response[..read].starts_with(b"HTTP/1.0 200")
    }

    fn register_startup(executable: &Path) -> Result<(), String> {
        let mut key: HKEY = ptr::null_mut();
        let key_path = wide(RUN_KEY);
        let mut disposition = 0;
        // SAFETY: The registry path and output pointers are valid for the call.
        let status = unsafe {
            RegCreateKeyExW(
                HKEY_CURRENT_USER,
                key_path.as_ptr(),
                0,
                ptr::null(),
                0,
                KEY_SET_VALUE,
                ptr::null(),
                &mut key,
                &mut disposition,
            )
        };
        if status != 0 {
            return Err(format!(
                "failed to open Windows startup registry key: error {status}"
            ));
        }
        let value_name = wide(RUN_VALUE);
        let command = wide(&format!("\"{}\" --login-start", executable.display()));
        // SAFETY: The registry key is open and command contains a terminating UTF-16 nul.
        let status = unsafe {
            RegSetValueExW(
                key,
                value_name.as_ptr(),
                0,
                REG_SZ,
                command.as_ptr().cast(),
                (command.len() * std::mem::size_of::<u16>()) as u32,
            )
        };
        // SAFETY: This function owns the registry handle returned above.
        unsafe { RegCloseKey(key) };
        if status != 0 {
            return Err(format!("failed to enable Weaver startup: error {status}"));
        }
        Ok(())
    }

    fn unregister_startup() -> Result<(), String> {
        let mut key: HKEY = ptr::null_mut();
        let key_path = wide(RUN_KEY);
        // SAFETY: The registry path and output key pointer are valid for the call.
        let status = unsafe {
            RegOpenKeyExW(
                HKEY_CURRENT_USER,
                key_path.as_ptr(),
                0,
                KEY_SET_VALUE,
                &mut key,
            )
        };
        if status != 0 {
            return Ok(());
        }
        let value_name = wide(RUN_VALUE);
        // SAFETY: The key is open and the value name is nul-terminated.
        let status = unsafe { RegDeleteValueW(key, value_name.as_ptr()) };
        // SAFETY: This function owns the registry handle returned above.
        unsafe { RegCloseKey(key) };
        if status != 0 && status != 2 {
            return Err(format!("failed to disable Weaver startup: error {status}"));
        }
        Ok(())
    }

    fn startup_enabled() -> Result<bool, String> {
        let mut key: HKEY = ptr::null_mut();
        let key_path = wide(RUN_KEY);
        // SAFETY: The registry path and output key pointer are valid for the call.
        let status = unsafe {
            RegOpenKeyExW(
                HKEY_CURRENT_USER,
                key_path.as_ptr(),
                0,
                KEY_QUERY_VALUE,
                &mut key,
            )
        };
        if status != 0 {
            return Ok(false);
        }
        let value_name = wide(RUN_VALUE);
        // SAFETY: The key is open and we only query the value's metadata.
        let status = unsafe {
            RegQueryValueExW(
                key,
                value_name.as_ptr(),
                ptr::null(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        // SAFETY: This function owns the registry handle returned above.
        unsafe { RegCloseKey(key) };
        Ok(status == 0)
    }

    fn open_target(target: &str) -> Result<(), String> {
        let verb = wide("open");
        let target = wide(target);
        // SAFETY: Both strings are nul-terminated and remain live through the shell call.
        let result = unsafe {
            ShellExecuteW(
                ptr::null_mut(),
                verb.as_ptr(),
                target.as_ptr(),
                ptr::null(),
                ptr::null(),
                SW_SHOWNORMAL,
            )
        } as isize;
        if result <= 32 {
            return Err(format!(
                "Windows could not open {target:?}; ShellExecute error {result}"
            ));
        }
        Ok(())
    }

    pub(super) fn show_error(title: &str, message: &str) {
        let title = wide(title);
        let message = wide(message);
        // SAFETY: The message buffers are nul-terminated and remain live through the dialog call.
        unsafe {
            MessageBoxW(
                ptr::null_mut(),
                message.as_ptr(),
                title.as_ptr(),
                MB_ICONERROR | MB_OK,
            );
        }
    }

    fn wide(value: &str) -> Vec<u16> {
        std::ffi::OsStr::new(value)
            .encode_wide()
            .chain(Some(0))
            .collect()
    }

    fn write_wide_buffer(buffer: &mut [u16], value: &str) {
        let encoded = std::ffi::OsStr::new(value).encode_wide();
        for (slot, value) in buffer.iter_mut().zip(encoded) {
            *slot = value;
        }
    }

    #[cfg(test)]
    mod tests {
        use std::path::Path;

        use super::{desktop_profile_dir_from, tray_mutex_name_for_user};

        #[test]
        fn desktop_profile_is_isolated_from_legacy_portable_state() {
            let local_app_data = Path::new(r"C:\\")
                .join("Users")
                .join("example")
                .join("AppData")
                .join("Local");

            assert_eq!(
                desktop_profile_dir_from(&local_app_data),
                local_app_data.join("ScryerMedia").join("Weaver")
            );
        }

        #[test]
        fn tray_mutex_is_global_but_scoped_to_one_windows_user() {
            assert_eq!(
                tray_mutex_name_for_user("EXAMPLE-PC", "example"),
                "Global\\ScryerMedia.Weaver.Desktop.v1.Tray.004500580041004D0050004C0045002D00500043005C006500780061006D0070006C0065"
            );
        }
    }
}
