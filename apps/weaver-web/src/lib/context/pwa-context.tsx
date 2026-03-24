import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  type ReactNode,
} from "react";

type PwaContextValue = {
  updateAvailable: false;
  applyUpdate: () => void;
};

const PwaContext = createContext<PwaContextValue | null>(null);

const UPDATE_CHECK_INTERVAL_MS = 6 * 60 * 60 * 1000;
const VISIBILITY_CHECK_COOLDOWN_MS = 60 * 60 * 1000;

function canRegisterServiceWorker() {
  return (
    import.meta.env.PROD &&
    typeof window !== "undefined" &&
    "serviceWorker" in navigator
  );
}

export function PwaProvider({ children }: { children: ReactNode }) {
  const reloadingRef = useRef(false);
  const lastUpdateCheckRef = useRef(0);

  useEffect(() => {
    if (!canRegisterServiceWorker()) {
      return undefined;
    }

    let activeRegistration: ServiceWorkerRegistration | null = null;
    let disposed = false;
    let updateTimer: number | null = null;
    const serviceWorkerUrl = `${import.meta.env.BASE_URL}sw.js`;

    // Auto-apply updates: tell the waiting worker to activate immediately
    // and reload the page. No user prompt needed for a local network app.
    const autoApplyUpdate = (worker: ServiceWorker) => {
      if (disposed || reloadingRef.current) {
        return;
      }
      reloadingRef.current = true;
      worker.postMessage({ type: "SKIP_WAITING" });
    };

    const triggerUpdateCheck = () => {
      if (!activeRegistration) {
        return;
      }
      lastUpdateCheckRef.current = Date.now();
      void activeRegistration.update();
    };

    const watchInstallingWorker = (registration: ServiceWorkerRegistration) => {
      const installing = registration.installing;
      if (!installing) {
        return;
      }

      installing.addEventListener("statechange", () => {
        if (
          installing.state === "installed" &&
          navigator.serviceWorker.controller
        ) {
          autoApplyUpdate(installing);
        }
      });
    };

    const handleControllerChange = () => {
      if (!reloadingRef.current) {
        return;
      }
      window.location.reload();
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState !== "visible") {
        return;
      }
      if (Date.now() - lastUpdateCheckRef.current < VISIBILITY_CHECK_COOLDOWN_MS) {
        return;
      }
      triggerUpdateCheck();
    };

    navigator.serviceWorker.addEventListener("controllerchange", handleControllerChange);
    document.addEventListener("visibilitychange", handleVisibilityChange);

    void navigator.serviceWorker.register(serviceWorkerUrl).then((registration) => {
      if (disposed) {
        return;
      }

      activeRegistration = registration;

      // If a worker is already waiting, apply immediately.
      if (navigator.serviceWorker.controller && registration.waiting) {
        autoApplyUpdate(registration.waiting);
      }
      watchInstallingWorker(registration);

      registration.addEventListener("updatefound", () => {
        watchInstallingWorker(registration);
      });

      lastUpdateCheckRef.current = Date.now();
      updateTimer = window.setInterval(triggerUpdateCheck, UPDATE_CHECK_INTERVAL_MS);
    });

    return () => {
      disposed = true;
      if (updateTimer !== null) {
        window.clearInterval(updateTimer);
      }
      navigator.serviceWorker.removeEventListener(
        "controllerchange",
        handleControllerChange,
      );
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, []);

  const value = useMemo<PwaContextValue>(
    () => ({
      updateAvailable: false,
      applyUpdate: () => {},
    }),
    [],
  );

  return <PwaContext.Provider value={value}>{children}</PwaContext.Provider>;
}

export function usePwa() {
  const context = useContext(PwaContext);
  if (!context) {
    throw new Error("usePwa must be used within PwaProvider");
  }
  return context;
}
