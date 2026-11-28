// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { BookOpen, MessagesSquare, Palette } from "lucide-react";
import {
  memo,
  useEffect,
  useId,
  useRef,
  useState,
  type KeyboardEvent,
} from "react";
import { gql, useQuery } from "urql";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { AppearancePicker } from "../components/AppearancePicker";
import { Icon } from "../components/icons";
// These logos are reused verbatim from Scryer's public/social assets.
import discordLogo from "./social/discord.svg";
import redditLogo from "./social/reddit.svg";

const APPEARANCE_ACCESS_QUERY = gql`
  query RailAppearanceAccess {
    callerScope
  }
`;

const SHORTCUT =
  "flex h-8 min-w-0 cursor-pointer items-center justify-center gap-1.5 border border-[color:var(--wv-rail-frame)] bg-[var(--wv-rail-panel)] px-2 text-[12px] text-wv-secondary hover:border-wv-control/70 hover:bg-wv-chrome hover:text-wv-fg aria-expanded:border-[color:var(--wv-rail-accent)] aria-expanded:bg-[var(--wv-rail-selection)]";
const PANEL =
  "border border-[color:var(--wv-rail-frame)] bg-[var(--wv-rail-panel)] shadow-wv-menu";
const SOCIAL_LINKS = [
  {
    name: "Reddit",
    testId: "shell-rail-reddit",
    href: "https://www.reddit.com/r/scryer_media/",
    logo: redditLogo,
  },
  {
    name: "Discord",
    testId: "shell-rail-discord",
    href: "https://discord.gg/SQmtZTanqm",
    logo: discordLogo,
  },
];

type Panel = "theme" | "social" | "social-hover" | null;

function RailAppearanceOptions() {
  const [{ data, error }] = useQuery<{
    callerScope: "ADMIN" | "CONTROL" | "READ";
  }>({ query: APPEARANCE_ACCESS_QUERY });
  return (
    <AppearancePicker
      editable={!error && data?.callerScope === "ADMIN"}
      idPrefix="rail-appearance"
      compact
    />
  );
}

function navigateMenu(event: KeyboardEvent<HTMLDivElement>) {
  const items = Array.from(
    event.currentTarget.querySelectorAll<HTMLAnchorElement>(
      '[role="menuitem"]',
    ),
  );
  const active = items.findIndex((item) => item === document.activeElement);
  let next: number;
  switch (event.key) {
    case "ArrowDown":
      next = (active + 1) % items.length;
      break;
    case "ArrowUp":
      next = (active + items.length - 1) % items.length;
      break;
    case "Home":
      next = 0;
      break;
    case "End":
      next = items.length - 1;
      break;
    default:
      return;
  }
  event.preventDefault();
  items[next]?.focus();
}

export const RailShortcuts = memo(function RailShortcuts({
  onNavigate,
}: {
  onNavigate?: () => void;
}) {
  const t = useTranslate();
  const [open, setOpen] = useState<Panel>(null);
  const themeOpen = open === "theme";
  const socialOpen = open === "social" || open === "social-hover";
  const themeId = useId();
  const socialId = useId();
  const themeTrigger = useRef<HTMLButtonElement>(null);
  const themePanel = useRef<HTMLDivElement>(null);
  const socialTrigger = useRef<HTMLButtonElement>(null);
  const socialArea = useRef<HTMLDivElement>(null);
  const firstSocialLink = useRef<HTMLAnchorElement>(null);

  useEffect(() => {
    if (open === "theme") {
      themePanel.current?.focus({ preventScroll: true });
    } else if (open === "social") {
      firstSocialLink.current?.focus({ preventScroll: true });
    }
  }, [open]);

  useEffect(() => {
    if (open === null) return;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node;
      const inside =
        open === "theme"
          ? themeTrigger.current?.contains(target) ||
            themePanel.current?.contains(target)
          : socialArea.current?.contains(target);
      if (!inside) setOpen(null);
    };
    const onKeyDown = (event: globalThis.KeyboardEvent) => {
      if (event.key !== "Escape") return;
      // In the drawer, Escape closes this panel before it closes the drawer.
      event.preventDefault();
      event.stopPropagation();
      if (open === "theme") themeTrigger.current?.focus();
      if (open === "social") socialTrigger.current?.focus();
      setOpen(null);
    };
    document.addEventListener("pointerdown", onPointerDown, true);
    document.addEventListener("keydown", onKeyDown, true);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown, true);
      document.removeEventListener("keydown", onKeyDown, true);
    };
  }, [open]);

  const followLink = () => {
    setOpen(null);
    onNavigate?.();
  };

  return (
    <div
      data-testid="shell-rail-shortcuts"
      className="wv-rail-shortcuts relative grid flex-none grid-cols-2 gap-1.5 border-t border-wv-line-strong p-2.5"
      onBlur={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) setOpen(null);
      }}
    >
      <a
        href="https://www.scryer.media/weaver/donate/"
        target="_blank"
        rel="noopener noreferrer"
        data-testid="shell-rail-sponsor"
        className={cn(SHORTCUT, "wv-rail-sponsor")}
        onClick={followLink}
      >
        <Icon name="sponsor" size={14} className="shrink-0" />
        <span className="truncate">{t("nav.sponsor")}</span>
      </a>
      <button
        ref={themeTrigger}
        type="button"
        aria-haspopup="dialog"
        aria-expanded={themeOpen}
        aria-controls={themeOpen ? themeId : undefined}
        data-testid="shell-rail-theme"
        className={SHORTCUT}
        onClick={() => setOpen(themeOpen ? null : "theme")}
      >
        <Palette aria-hidden size={14} className="shrink-0" />
        <span className="truncate">{t("next.shell.theme")}</span>
      </button>
      <a
        href="https://www.scryer.media/weaver/docs/"
        target="_blank"
        rel="noopener noreferrer"
        data-testid="shell-rail-docs"
        className={SHORTCUT}
        onClick={followLink}
      >
        <BookOpen aria-hidden size={14} className="shrink-0" />
        <span className="truncate">{t("next.shell.docs")}</span>
      </a>
      <div
        ref={socialArea}
        className="relative min-w-0"
        onPointerLeave={() => {
          setOpen((current) => (current === "social-hover" ? null : current));
        }}
      >
        <button
          ref={socialTrigger}
          type="button"
          aria-haspopup="menu"
          aria-expanded={socialOpen}
          aria-controls={socialOpen ? socialId : undefined}
          data-testid="shell-rail-social"
          className={cn(SHORTCUT, "w-full")}
          onPointerEnter={(event) => {
            if (event.pointerType === "mouse") {
              setOpen((current) => current ?? "social-hover");
            }
          }}
          onClick={() => setOpen(open === "social" ? null : "social")}
        >
          <MessagesSquare aria-hidden size={14} className="shrink-0" />
          <span className="truncate">{t("next.shell.social")}</span>
        </button>
        {socialOpen ? (
          <div className="absolute right-0 bottom-full z-30 w-40 pb-1.5 lg:right-auto lg:bottom-0 lg:left-full lg:pb-0 lg:pl-1.5">
            <div
              id={socialId}
              role="menu"
              aria-label={t("next.shell.social")}
              data-testid="shell-rail-social-menu"
              className={cn(PANEL, "p-1")}
              onKeyDown={navigateMenu}
            >
              {SOCIAL_LINKS.map(({ href, logo, name, testId }, index) => (
                <a
                  key={name}
                  ref={index === 0 ? firstSocialLink : undefined}
                  href={href}
                  target="_blank"
                  rel="noopener noreferrer"
                  role="menuitem"
                  tabIndex={-1}
                  data-testid={testId}
                  className="flex items-center gap-2 px-2 py-1.5 text-[12px] text-wv-secondary hover:bg-wv-menu-hover hover:text-wv-fg"
                  onClick={followLink}
                >
                  <img
                    src={logo}
                    alt=""
                    aria-hidden="true"
                    className="size-4 shrink-0 object-contain"
                  />
                  <span>{name}</span>
                  <Icon
                    name="external"
                    size={13}
                    className="ml-auto shrink-0"
                  />
                </a>
              ))}
            </div>
          </div>
        ) : null}
      </div>
      {themeOpen ? (
        <div
          ref={themePanel}
          id={themeId}
          role="dialog"
          aria-labelledby={`${themeId}-title`}
          tabIndex={-1}
          data-testid="shell-rail-theme-popover"
          className={cn(
            PANEL,
            "absolute inset-x-2.5 bottom-full z-30 max-h-[calc(100dvh-6rem)] overflow-y-auto overscroll-contain p-1.5 outline-none",
          )}
        >
          <h2
            id={`${themeId}-title`}
            className="mb-1.5 border-b border-wv-hairline px-1.5 pt-0.5 pb-1.5 text-xs font-semibold text-wv-fg"
          >
            {t("next.appearance.title")}
          </h2>
          <RailAppearanceOptions />
        </div>
      ) : null}
    </div>
  );
});
