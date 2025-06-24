// scripts/docker-utils.ts
import { execSync, spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

export const DOCKER_CONTAINER_NAME = 'schedule-f-postgres';
export const DOCKER_PORT = 5432;
export const DOCKER_PASSWORD = 'localdevpassword';
export const DOCKER_DB_NAME = 'schedule_f_dev';

export function isDockerRunning(): boolean {
  try {
    execSync('docker info', { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

export function isContainerRunning(): boolean {
  try {
    const result = execSync(`docker ps --filter "name=${DOCKER_CONTAINER_NAME}" --format "{{.Names}}"`, {
      encoding: 'utf-8'
    });
    return result.trim() === DOCKER_CONTAINER_NAME;
  } catch {
    return false;
  }
}

export function doesContainerExist(): boolean {
  try {
    const result = execSync(`docker ps -a --filter "name=${DOCKER_CONTAINER_NAME}" --format "{{.Names}}"`, {
      encoding: 'utf-8'
    });
    return result.trim() === DOCKER_CONTAINER_NAME;
  } catch {
    return false;
  }
}

export function startDocker(): void {
  console.log('🐳 Starting Docker Desktop...');
  
  if (process.platform === 'darwin') {
    // macOS
    execSync('open -a Docker');
  } else if (process.platform === 'win32') {
    // Windows
    execSync('start "" "C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe"');
  } else {
    // Linux
    console.log('Please start Docker manually on Linux');
  }
  
  // Wait for Docker to be ready
  let attempts = 0;
  while (!isDockerRunning() && attempts < 30) {
    console.log('⏳ Waiting for Docker to start...');
    execSync('sleep 2');
    attempts++;
  }
  
  if (!isDockerRunning()) {
    throw new Error('Docker failed to start after 60 seconds');
  }
  
  console.log('✅ Docker is running');
}

export function startPostgresContainer(): void {
  if (!isDockerRunning()) {
    startDocker();
  }
  
  if (isContainerRunning()) {
    console.log('✅ PostgreSQL container is already running');
    return;
  }
  
  if (doesContainerExist()) {
    console.log('🔄 Starting existing PostgreSQL container...');
    execSync(`docker start ${DOCKER_CONTAINER_NAME}`);
  } else {
    console.log('🚀 Creating new PostgreSQL container...');
    execSync(`docker run -d \
      --name ${DOCKER_CONTAINER_NAME} \
      -e POSTGRES_PASSWORD=${DOCKER_PASSWORD} \
      -e POSTGRES_DB=${DOCKER_DB_NAME} \
      -p ${DOCKER_PORT}:5432 \
      -v schedule-f-postgres-data:/var/lib/postgresql/data \
      postgres:15-alpine`
    );
  }
  
  // Wait for PostgreSQL to be ready
  console.log('⏳ Waiting for PostgreSQL to be ready...');
  let attempts = 0;
  while (attempts < 30) {
    try {
      execSync(`docker exec ${DOCKER_CONTAINER_NAME} pg_isready -U postgres`, { stdio: 'ignore' });
      console.log('✅ PostgreSQL is ready');
      break;
    } catch {
      execSync('sleep 1');
      attempts++;
    }
  }
  
  if (attempts >= 30) {
    throw new Error('PostgreSQL failed to start');
  }
}

export function stopPostgresContainer(): void {
  if (isContainerRunning()) {
    console.log('🛑 Stopping PostgreSQL container...');
    execSync(`docker stop ${DOCKER_CONTAINER_NAME}`);
    console.log('✅ PostgreSQL container stopped');
  }
}