import { createBrowserRouter } from "react-router";
import { Layout } from "@/components/Layout";
import { JobList } from "@/pages/JobList";
import { JobDetail } from "@/pages/JobDetail";
import { Upload } from "@/pages/Upload";
import { Servers } from "@/pages/Servers";
import { Settings } from "@/pages/Settings";
import { History } from "@/pages/History";

export const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { index: true, element: <JobList /> },
      { path: "jobs/:id", element: <JobDetail /> },
      { path: "upload", element: <Upload /> },
      { path: "history", element: <History /> },
      { path: "servers", element: <Servers /> },
      { path: "settings", element: <Settings /> },
    ],
  },
]);
