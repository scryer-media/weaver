import { createBrowserRouter, Navigate } from "react-router";
import { Layout } from "@/components/Layout";
import { JobList } from "@/pages/JobList";
import { JobDetail } from "@/pages/JobDetail";
import { Upload } from "@/pages/Upload";
import { Servers } from "@/pages/Servers";
import { Categories } from "@/pages/Categories";
import { History } from "@/pages/History";
import { MetricsPage } from "@/pages/MetricsPage";
import { SettingsLayout } from "@/pages/settings/SettingsLayout";
import { GeneralSettingsPage } from "@/pages/settings/GeneralSettingsPage";
import { SecuritySettingsPage } from "@/pages/settings/SecuritySettingsPage";
import { BackupSettingsPage } from "@/pages/settings/BackupSettingsPage";
import { RssSettingsPage } from "@/pages/settings/RssSettingsPage";
import { ScheduleSettingsPage } from "@/pages/settings/ScheduleSettingsPage";

const basename = window.__WEAVER_BASE__ || "/";

export const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { index: true, element: <JobList /> },
      { path: "jobs/:id", element: <JobDetail /> },
      { path: "upload", element: <Upload /> },
      { path: "metrics", element: <MetricsPage /> },
      { path: "history", element: <History /> },
      { path: "servers", element: <Navigate to="/settings/servers" replace /> },
      { path: "categories", element: <Navigate to="/settings/categories" replace /> },
      {
        path: "settings",
        element: <SettingsLayout />,
        children: [
          { index: true, element: <Navigate to="general" replace /> },
          { path: "general", element: <GeneralSettingsPage /> },
          { path: "security", element: <SecuritySettingsPage /> },
          { path: "backup", element: <BackupSettingsPage /> },
          { path: "rss", element: <RssSettingsPage /> },
          { path: "schedules", element: <ScheduleSettingsPage /> },
          { path: "categories", element: <Categories embedded /> },
          { path: "servers", element: <Servers embedded /> },
        ],
      },
    ],
  },
], { basename });
