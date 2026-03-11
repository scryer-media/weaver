import { Outlet } from "react-router";

export const settingsNav = [
  {
    to: "/settings/general",
    labelKey: "settings.general",
    descriptionKey: "settings.generalPageDesc",
  },
  {
    to: "/settings/security",
    labelKey: "settings.security",
    descriptionKey: "settings.securityDesc",
  },
  {
    to: "/settings/backup",
    labelKey: "settings.backupNav",
    descriptionKey: "settings.backupPageDesc",
  },
  {
    to: "/settings/categories",
    labelKey: "categories.title",
    descriptionKey: "settings.categoriesDesc",
  },
  {
    to: "/settings/servers",
    labelKey: "servers.title",
    descriptionKey: "settings.serversDesc",
  },
];

export function SettingsLayout() {
  return <Outlet />;
}
