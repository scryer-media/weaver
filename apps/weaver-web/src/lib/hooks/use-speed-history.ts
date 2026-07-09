import { useEffect, useRef, useState } from "react";

/**
 * Rolling ring-buffer of recent download-speed samples for the sidebar sparkline.
 * Samples the latest live speed on a fixed interval (decoupled from the render
 * cadence of the always-mounted shell), keeping the last `length` points.
 */
export function useSpeedHistory(speed: number, length = 60, intervalMs = 1000): number[] {
  const speedRef = useRef(speed);
  const [history, setHistory] = useState<number[]>(() => Array<number>(length).fill(0));

  useEffect(() => {
    speedRef.current = speed;
  }, [speed]);

  useEffect(() => {
    const id = setInterval(() => {
      setHistory((previous) => {
        const next = previous.length >= length ? previous.slice(previous.length - length + 1) : previous.slice();
        next.push(speedRef.current);
        return next;
      });
    }, intervalMs);
    return () => clearInterval(id);
  }, [length, intervalMs]);

  return history;
}
