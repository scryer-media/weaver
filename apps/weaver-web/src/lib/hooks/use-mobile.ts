import * as React from "react";

const MOBILE_BREAKPOINT = 768;

export function useIsMobile(breakpoint = MOBILE_BREAKPOINT) {
  const [isMobile, setIsMobile] = React.useState<boolean | undefined>(undefined);

  React.useEffect(() => {
    const mediaQuery = window.matchMedia(`(max-width: ${breakpoint - 1}px)`);
    const onChange = () => setIsMobile(window.innerWidth < breakpoint);

    mediaQuery.addEventListener("change", onChange);
    setIsMobile(window.innerWidth < breakpoint);

    return () => mediaQuery.removeEventListener("change", onChange);
  }, [breakpoint]);

  return !!isMobile;
}
