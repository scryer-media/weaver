import { gql } from "urql";

/**
 * Queries this UI asks for in its own shape.
 *
 * Everything else comes from `@/graphql/queries`. These read fields the shared
 * fragments leave out — history rows carry how long a job took, and a failed
 * one says why. Every field below is one `HistoryItem` already exposes.
 */

export const NEXT_HISTORY_PAGE_QUERY = gql`
  query NextHistoryPage($input: HistoryPageInput!) {
    historyPage(input: $input) {
      items {
        id
        name
        displayTitle
        originalTitle
        status: state
        error
        totalBytes
        downloadedBytes
        health
        hasPassword
        category
        createdAt
        completedAt
        deleteOperation {
          operationId
          state
          locked
          deleteFiles
          errorMessage
        }
      }
      totalCount
      counts {
        all
        success
        failure
      }
      categoryCounts {
        category
        count
      }
    }
  }
`;
