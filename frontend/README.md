# Schedule F Frontend

This is a [Next.js](https://nextjs.org/) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Project Overview

The Schedule F Frontend is a web application that provides comprehensive analysis and visualization of public comments on the proposed "Schedule F" regulation - "Improving Performance, Accountability and Responsiveness in the Civil Service". This application enables users to:

- Browse and search through thousands of public comments submitted to regulations.gov
- View AI-analyzed sentiment (For/Against/Neutral) for each comment
- Explore key themes and quotes extracted from comments
- Identify duplicate/similar comments through advanced deduplication
- Visualize comment clusters using PCA (Principal Component Analysis)
- Track stance changes over time with interactive charts
- Filter and export data for further analysis

The application serves researchers, policymakers, journalists, and citizens interested in understanding public opinion on civil service reform proposals.

## Getting Started

### Prerequisites

- Node.js (version 18+ recommended)
- npm (package manager)
- PostgreSQL database (for local development)

### Installation

1.  Clone the repository:
    ```bash
    git clone <repository-url>
    cd schedule-f-frontend
    ```
2.  Install dependencies:
    ```bash
    npm install
    # or
    # yarn install
    # or
    # pnpm install
    # or
    # bun install
    ```

### Environment Setup

1.  Create a `.env` file in the root directory with the following variables:

```bash
# Database Environment (local, preprod, or prod)
DB_ENV=local

# Database URLs for each environment
DATABASE_URL_LOCAL=postgresql://username:password@localhost:5432/schedule_f_dev
DATABASE_URL_PREPROD=postgresql://username:password@host:5432/schedule_f_preprod
DATABASE_URL_PROD=postgresql://username:password@host:5432/schedule_f_prod

# Optional: Path to custom env file
ENV_PATH=.env
```

2.  For local development, you can set up a PostgreSQL database using the provided script:
    ```bash
    npm run ensure-local-db
    ```

3.  Push the database schema to your local database:
    ```bash
    npm run db:push:local
    ```

4.  (Optional) Seed the database with sample data:
    ```bash
    npm run db:seed:local
    ```

### Running the Development Server

To start the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

## Key Features & Architecture

This project leverages several modern web development practices and technologies:

### Core Features
*   **Comment Analysis Dashboard**: Main interface displays public comments with AI-analyzed stance (For/Against/Neutral), key quotes, themes, and rationale
*   **Advanced Deduplication**: Groups similar comments together with duplicate counts, helping identify mass campaigns or form letters
*   **Cluster Visualization**: Interactive PCA-based visualization showing comment clusters with descriptive titles and summaries
*   **Time Series Analysis**: Track stance changes over time with interactive charts showing comment volume by position
*   **Rich Filtering**: Filter by stance, themes, organization, dates, and search across comment text
*   **Data Export**: Export filtered results for further analysis

### Technical Architecture
*   **Next.js App Router**: Uses Next.js 15 with App Router for server-side rendering and optimal performance
*   **Database**: PostgreSQL with Drizzle ORM (local Docker container for development, Neon Database for production)
*   **State Management**: Context-based state management for filters, pagination, and search
*   **Caching**: Next.js built-in caching with custom in-memory LRU cache for data optimization
*   **UI Components**: Modular component architecture with responsive design using Tailwind CSS
*   **Data Visualization**: Nivo for interactive charts and cluster visualizations
*   **Testing**: Jest test suite for unit and integration tests

## Project Structure

A brief overview of the key directories:

*   `src/app/`: Next.js App Router pages and routes
    *   `/` - Redirects to paginated comment view
    *   `/page/[page]/size/[size]` - Main comment dashboard with pagination
    *   `/clusters` - Cluster visualization page
    *   `/comment/[id]` - Individual comment detail view
    *   `/attribution` - Attribution and credits page
*   `src/components/`: Reusable React components
    *   `ServerCommentTable/` - Main data table with filtering, sorting, and export
    *   `ClusterVisualization/` - Interactive PCA cluster chart
    *   `StanceOverTime/` - Time series chart component
    *   `FilterModal/` - Advanced filtering interface
    *   `ui/` - Shared UI components (Button, Card, Badge, etc.)
*   `src/lib/`: Core application logic
    *   `db/` - Database schema and configuration (Drizzle ORM)
    *   `actions/` - Server actions for data fetching
    *   `cache.ts` - Caching implementation
    *   `config.ts` - Application configuration and field definitions
*   `scripts/`: Utility scripts
    *   `ensure-local-db.ts` - Sets up local PostgreSQL database
    *   `update-data.ts` - Updates comment data from source
    *   `load-cluster-descriptions.ts` - Loads cluster metadata
    *   `warm-cache.ts` - Pre-populates cache for better performance

## Available Scripts

The `package.json` file defines the following scripts:

*   `npm run dev`: Starts the Next.js development server.
*   `npm run build`: Builds the application for production.
*   `npm run start`: Starts the Next.js production server (after building).
*   `npm run lint`: Lints the codebase using Next.js's ESLint configuration.
*   `npm run test`: Runs the Jest test suite.
*   `npm run test:watch`: Runs Jest in watch mode.
*   `npm run db:push:local`: Pushes your Drizzle schema directly to the local database
*   `npm run db:push:preprod`: Pushes schema to preprod database
*   `npm run db:push:prod`: Pushes schema to production database
*   `npm run studio`: Opens Drizzle Studio for browsing your local database
*   `npm run db:seed:local`: Seeds the local database with sample data
*   `npm run db:load-clusters:local`: Loads cluster descriptions into the local database
*   `npm run db:ensure:local`: Ensures local Docker PostgreSQL container is running
*   `npm run db:status`: Shows current database connection status

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Additional Resources

- [Drizzle ORM Documentation](https://orm.drizzle.team/) - Learn about the database ORM used in this project
- [Neon Database](https://neon.tech/) - Serverless PostgreSQL used in production
- [Tailwind CSS Documentation](https://tailwindcss.com/docs) - For styling and UI components
- [Nivo Documentation](https://nivo.rocks/) - For data visualization components
- [Next.js Caching](https://nextjs.org/docs/app/building-your-application/caching) - Understanding Next.js built-in caching mechanisms