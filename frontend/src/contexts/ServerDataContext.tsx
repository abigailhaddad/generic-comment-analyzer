"use client";

import {
  createContext,
  useContext,
  useState,
  useEffect,
  useRef,
  ReactNode,
} from "react";
import { Comment } from "@/lib/db/schema";
import { useRouter, useSearchParams, usePathname } from "next/navigation";
import { SortingState } from "@/components/ServerCommentTable/types";
import {
  getPaginatedComments,
  getCommentStatistics,
  getDedupedCommentStatistics,
  parseUrlToQueryOptions,
  getStanceTimeSeries,
} from "@/lib/actions/comments";
import { StanceData } from "@/components/StanceOverTime/types";

// Define the structure for stance time series data
interface StanceChartData {
  posted_date: StanceData[];
  received_date: StanceData[];
  posted_date_no_duplicates: StanceData[]; // New
  received_date_no_duplicates: StanceData[]; // New
  error?: string;
}

interface ServerDataContextProps {
  // Data
  data: Comment[];
  totalItems: number;

  // Statistics
  stats: {
    total: number;
    for: number;
    against: number;
    neutral: number;
  };

  // Deduped Statistics
  dedupedStats: {
    total: number;
    for: number;
    against: number;
    neutral: number;
  };

  // Stance Time Series Data
  stanceTimeSeriesData: StanceChartData | null;

  // State
  loading: boolean;
  error: string | null;

  // Filter state
  filters: Record<string, unknown>;
  setFilters: (filters: Record<string, unknown>) => void;

  // Search state
  searchQuery: string;
  setSearchQuery: (query: string) => void;

  // Sorting state
  sorting: SortingState | undefined;
  setSorting: (sorting: SortingState | undefined) => void;
  handleSort: (column: string) => void;

  // Pagination state
  pageSize: number;
  setPageSize: (size: number) => void;
  currentPage: number;
  setCurrentPage: (page: number) => void;
  totalPages: number;
  goToPage: (page: number) => void;
  nextPage: () => void;
  previousPage: () => void;
  canNextPage: boolean;
  canPreviousPage: boolean;

  // Additional utilities
  refreshData: () => Promise<void>;
  exportCSV: () => void;
}

interface ServerDataContextProviderProps {
  children: ReactNode;
  initialPage?: number;
  initialPageSize?: number;
}

const ServerDataContext = createContext<ServerDataContextProps | undefined>(
  undefined
);

/**
 * Provider that fetches data from the server based on URL parameters
 * and provides it to the application
 */
export function ServerDataContextProvider({
  children,
  initialPage = 1,
  initialPageSize = 10,
}: ServerDataContextProviderProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const pathname = usePathname();
  
  // Refs to track the latest fetch requests
  const fetchIdRef = useRef(0);
  const stanceFetchIdRef = useRef(0);

  // Extract URL parameters
  const urlSort = searchParams.get("sort");
  const urlSortDirection = searchParams.get("sortDirection") as
    | "asc"
    | "desc"
    | null;
  const urlSearch = searchParams.get("search") || "";

  // Use page and size from props (which come from route params)
  const currentPage = initialPage;
  const pageSize = initialPageSize;

  // Extract filter params from URL
  const getInitialFilters = (): Record<string, unknown> => {
    const initialFilters: Record<string, unknown> = {};

    for (const [key, value] of Array.from(searchParams.entries())) {
      if (key.startsWith("filter_")) {
        const filterKey = key.replace("filter_", "");

        // Try to parse JSON values (for arrays, etc)
        try {
          initialFilters[filterKey] = JSON.parse(value);
        } catch {
          // If not valid JSON, use as string
          initialFilters[filterKey] = value;
        }
      }
    }

    return initialFilters;
  };

  // Core state
  const [data, setData] = useState<Comment[]>([]);
  const [totalItems, setTotalItems] = useState(0);
  const [stats, setStats] = useState({
    total: 0,
    for: 0,
    against: 0,
    neutral: 0,
  });
  const [dedupedStats, setDedupedStats] = useState({
    total: 0,
    for: 0,
    against: 0,
    neutral: 0,
  });
  const [stanceTimeSeriesData, setStanceTimeSeriesData] =
    useState<StanceChartData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // UI state
  const [searchQuery, setSearchQuery] = useState(urlSearch);
  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState(urlSearch);
  const [filters, setFilters] = useState<Record<string, unknown>>(
    getInitialFilters()
  );
  const [sorting, setSorting] = useState<SortingState | undefined>(
    urlSort && urlSortDirection
      ? { column: urlSort, direction: urlSortDirection }
      : undefined
  );

  // Debounce search query
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearchQuery(searchQuery);
    }, 500); // 500ms delay

    return () => clearTimeout(timer);
  }, [searchQuery]);

  // Update the fetchStanceTimeSeries function
  const fetchStanceTimeSeries = async () => {
    // Increment stance fetch ID to track the latest request
    const currentStanceFetchId = ++stanceFetchIdRef.current;
    
    try {
      // Create options with current filters and debounced search
      const stanceChartOptions = {
        filters: filters,
        search: debouncedSearchQuery || undefined,
        searchFields: undefined,
      };

      // Fetch all four versions in parallel
      const [
        stancePostedResponse,
        stanceReceivedResponse,
        stancePostedNoDupsResponse,
        stanceReceivedNoDupsResponse,
      ] = await Promise.all([
        getStanceTimeSeries(stanceChartOptions, "postedDate", true),
        getStanceTimeSeries(stanceChartOptions, "receivedDate", true),
        getStanceTimeSeries(stanceChartOptions, "postedDate", false),
        getStanceTimeSeries(stanceChartOptions, "receivedDate", false),
      ]);

      // Check if this is still the latest request
      if (currentStanceFetchId !== stanceFetchIdRef.current) {
        // A newer request has been initiated, discard these results
        return;
      }

      // Process and set stance time series data
      const newStanceData: StanceChartData = {
        posted_date: stancePostedResponse.success
          ? stancePostedResponse.data!
          : [],
        received_date: stanceReceivedResponse.success
          ? stanceReceivedResponse.data!
          : [],
        posted_date_no_duplicates: stancePostedNoDupsResponse.success
          ? stancePostedNoDupsResponse.data!
          : [],
        received_date_no_duplicates: stanceReceivedNoDupsResponse.success
          ? stanceReceivedNoDupsResponse.data!
          : [],
        error:
          stancePostedResponse.error ||
          stanceReceivedResponse.error ||
          stancePostedNoDupsResponse.error ||
          stanceReceivedNoDupsResponse.error ||
          undefined,
      };
      setStanceTimeSeriesData(newStanceData);
    } catch (err) {
      // Check if this is still the latest request before setting error
      if (currentStanceFetchId === stanceFetchIdRef.current) {
        console.error("Error fetching stance time series:", err);
        setStanceTimeSeriesData({
          posted_date: [],
          received_date: [],
          posted_date_no_duplicates: [],
          received_date_no_duplicates: [],
          error: "Failed to fetch time series data",
        });
      }
    }
  };

  // Fetch data based on current parameters
  const fetchData = async () => {
    // Increment fetch ID to track the latest request
    const currentFetchId = ++fetchIdRef.current;
    
    setLoading(true);

    try {
      // Create a simple object from search params, to avoid issues with URLSearchParams
      const paramsObj: Record<string, string> = {};
      searchParams.forEach((value, key) => {
        paramsObj[key] = value;
      });

      const options = await parseUrlToQueryOptions(paramsObj);

      // Override page and pageSize from state since they're no longer in search params
      options.page = currentPage;
      options.pageSize = pageSize;

      // Fetch data and stats in parallel (but not stance time series)
      const [dataResponse, statsResponse, dedupedStatsResponse] = await Promise.all([
        getPaginatedComments(options),
        getCommentStatistics(options),
        getDedupedCommentStatistics(options),
      ]);

      // Check if this is still the latest request
      if (currentFetchId !== fetchIdRef.current) {
        // A newer request has been initiated, discard these results
        return;
      }

      if (dataResponse.success && dataResponse.data) {
        // Debug logging for commentCount
        if (process.env.NODE_ENV === 'development' && dataResponse.data.length > 0) {
          console.log('Sample fetched comment data:', {
            firstComment: {
              id: dataResponse.data[0].id,
              title: dataResponse.data[0].title?.substring(0, 50),
              commentCount: dataResponse.data[0].commentCount,
              allKeys: Object.keys(dataResponse.data[0]).sort()
            },
            totalComments: dataResponse.data.length
          });
        }
        
        setData(dataResponse.data);
        setTotalItems(dataResponse.total || 0);
        setError(null);
      } else {
        setError(dataResponse.error || "Failed to fetch comments");
        console.error("Error fetching comments:", dataResponse.error);
      }

      if (statsResponse.success && statsResponse.stats) {
        setStats(statsResponse.stats);
      } else {
        console.error("Error fetching stats:", statsResponse.error);
      }

      if (dedupedStatsResponse.success && dedupedStatsResponse.stats) {
        setDedupedStats(dedupedStatsResponse.stats);
      } else {
        console.error("Error fetching deduped stats:", dedupedStatsResponse.error);
      }
    } catch (err) {
      // Check if this is still the latest request before setting error
      if (currentFetchId === fetchIdRef.current) {
        console.error("Exception in fetchData:", err);
        setError("An unexpected error occurred");
      }
    } finally {
      // Only set loading to false if this is still the latest request
      if (currentFetchId === fetchIdRef.current) {
        setLoading(false);
      }
    }
  };

  // Fetch stance time series on initial load
  useEffect(() => {
    fetchStanceTimeSeries();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // Only on mount

  // Refetch stance time series when filters or debounced search changes
  useEffect(() => {
    fetchStanceTimeSeries();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters, debouncedSearchQuery]); // Only when filters or debounced search changes

  // Refresh data function that can be called by consumers
  const refreshData = async () => {
    await fetchData();
  };

  // Use refs to track previous values
  const prevDebouncedSearchQuery = useRef(debouncedSearchQuery);
  const prevFilters = useRef(filters);

  // Update URL when filters, sorting or debounced search changes
  useEffect(() => {
    const searchChanged = prevDebouncedSearchQuery.current !== debouncedSearchQuery;
    const filtersChanged =
      JSON.stringify(prevFilters.current) !== JSON.stringify(filters);

    // Determine if we need to navigate to page 1
    const shouldNavigateToPage1 =
      (searchChanged || filtersChanged) && currentPage !== 1;

    // Create a new URLSearchParams object
    const params = new URLSearchParams();

    // Update sorting parameters
    if (sorting) {
      params.set("sort", sorting.column);
      params.set("sortDirection", sorting.direction);
    }

    // Update search parameter
    if (debouncedSearchQuery) {
      params.set("search", debouncedSearchQuery);
    }

    // Update filter parameters
    Object.entries(filters).forEach(([key, value]) => {
      if (
        value !== undefined &&
        value !== null &&
        !(Array.isArray(value) && value.length === 0) &&
        value !== ""
      ) {
        const stringValue =
          typeof value === "object" ? JSON.stringify(value) : String(value);
        params.set(`filter_${key}`, stringValue);
      }
    });

    // Build the correct path
    const queryString = params.toString();
    let fullPath: string;

    if (shouldNavigateToPage1) {
      // Navigate to page 1 with the new filters/search
      const newPath = `/page/1/size/${pageSize}`;
      fullPath = queryString ? `${newPath}?${queryString}` : newPath;
    } else {
      // Keep current page and size
      fullPath = queryString ? `${pathname}?${queryString}` : pathname;
    }

    // Update URL without refreshing page
    router.replace(fullPath, { scroll: false });

    // Update refs for next comparison
    prevDebouncedSearchQuery.current = debouncedSearchQuery;
    prevFilters.current = filters;
  }, [debouncedSearchQuery, sorting, filters, router, pathname, currentPage, pageSize]);

  // Fetch data when URL parameters change
  useEffect(() => {
    fetchData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams, currentPage, pageSize]);

  // Calculate total pages based on total items
  const totalPages = Math.ceil(totalItems / pageSize);

  // Pagination controls - navigate to new routes
  const goToPage = (page: number) => {
    const params = new URLSearchParams(searchParams.toString());
    const queryString = params.toString();
    const newPath = `/page/${page}/size/${pageSize}`;
    const fullPath = queryString ? `${newPath}?${queryString}` : newPath;
    router.push(fullPath, { scroll: false });
  };

  const setPageSize = (size: number) => {
    const params = new URLSearchParams(searchParams.toString());
    const queryString = params.toString();
    const newPath = `/page/1/size/${size}`;
    const fullPath = queryString ? `${newPath}?${queryString}` : newPath;
    router.push(fullPath, { scroll: false });
  };

  const setCurrentPage = (page: number) => {
    goToPage(page);
  };

  const nextPage = () => {
    if (canNextPage) {
      goToPage(currentPage + 1);
    }
  };

  const previousPage = () => {
    if (canPreviousPage) {
      goToPage(currentPage - 1);
    }
  };

  const canNextPage = currentPage < totalPages;
  const canPreviousPage = currentPage > 1;

  // Handle sorting
  const handleSort = (column: string) => {
    setSorting((prev: SortingState | undefined) => {
      if (prev?.column === column) {
        return {
          column,
          direction: prev.direction === "asc" ? "desc" : "asc",
        };
      }
      return {
        column,
        direction: "asc",
      };
    });

    // Navigate to page 1 when sorting changes
    if (currentPage !== 1) {
      goToPage(1);
    }
  };

  // Export the currently-filtered table to CSV
  const exportCSV = () => {
    if (data.length === 0) return;

    // Get all unique keys from the data
    const headerSet = new Set<string>();

    data.forEach((item) => {
      const record = item as Record<string, unknown>;
      Object.keys(record).forEach((key) => {
        const value = record[key];
        if (typeof value !== "object" || value === null) {
          headerSet.add(key);
        }
      });
    });

    const headers = Array.from(headerSet);
    if (headers.length === 0) return;

    // Build CSV text
    let csv = headers.map((h) => `"${h}"`).join(",") + "\n";

    data.forEach((item) => {
      const record = item as Record<string, unknown>;
      const row = headers.map((header) => {
        const value = record[header];

        if (value == null) return "";
        if (typeof value === "string") {
          // escape double quotes inside cell text
          return `"${value.replace(/"/g, '""')}"`;
        }
        return String(value);
      });

      csv += row.join(",") + "\n";
    });

    // Trigger a browser download
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);

    const link = document.createElement("a");
    link.href = url;
    link.download = "export.csv";
    link.style.display = "none";

    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  // Prepare context value
  const contextValue: ServerDataContextProps = {
    // Data
    data,
    totalItems,

    // Statistics
    stats,

    // Deduped Statistics
    dedupedStats,

    // Stance Time Series Data
    stanceTimeSeriesData,

    // State
    loading,
    error,

    // Filter state
    filters,
    setFilters,

    // Search state
    searchQuery,
    setSearchQuery,

    // Sorting state
    sorting,
    setSorting,
    handleSort,

    // Pagination state
    pageSize,
    setPageSize,
    currentPage,
    setCurrentPage,
    totalPages,
    goToPage,
    nextPage,
    previousPage,
    canNextPage,
    canPreviousPage,

    // Additional utilities
    refreshData,
    exportCSV,
  };

  return (
    <ServerDataContext.Provider value={contextValue}>
      {children}
    </ServerDataContext.Provider>
  );
}

export function useServerDataContext() {
  const context = useContext(ServerDataContext);
  if (context === undefined) {
    throw new Error(
      "useServerDataContext must be used within a ServerDataContextProvider"
    );
  }
  return context;
}
