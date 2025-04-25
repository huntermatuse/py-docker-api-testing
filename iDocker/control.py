import docker
import time
import atexit
import logging
import os
import io
import tarfile


class SQLServerContainer:
    def __init__(
        self,
        password="YourStrong%Passw0rd",
        port=1433,
        container_name="testing-mssql-container",
    ):
        self.password = password
        self.port = port
        self.container_name = container_name
        self.container = None
        self.client = None

        self.logger = logging.getLogger(__name__)

        atexit.register(self.cleanup)

    def start(self):
        """Start container using Docker SDK (preferred method)"""
        try:
            if not self.client:
                self.client = docker.from_env()

            try:
                existing = self.client.containers.get(self.container_name)
                if existing.status == "running":
                    self.logger.info(
                        f"Container '{self.container_name}' is already running."
                    )
                    self.container = existing
                    return True
                else:
                    self.logger.info(
                        f"Removing existing container '{self.container_name}'..."
                    )
                    existing.remove(force=True)
            except docker.errors.NotFound:
                pass

            self.logger.info(
                f"Starting SQL Server container '{self.container_name}'..."
            )

            self.container = self.client.containers.run(
                "mcr.microsoft.com/mssql/server:2019-latest",
                detach=True,
                name=self.container_name,
                environment={"ACCEPT_EULA": "Y", "SA_PASSWORD": self.password},
                ports={"1433/tcp": self.port},
            )

            self.logger.info(f"Container started with ID: {self.container.id[:12]}")

            self.logger.info("Waiting for SQL Server to be ready...")
            time.sleep(10)

            self.container.reload()
            if self.container.status == "running":
                self.logger.info("SQL Server container is running successfully!")
                return True
            else:
                self.logger.error(f"Container status: {self.container.status}")
                return False

        except docker.errors.APIError as e:
            self.logger.error(f"Docker API error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return False

    def stop(self):
        """Stop the container"""
        try:
            if self.container and self.client:
                self.logger.info(f"Stopping container '{self.container_name}'...")
                self.container.stop()
                self.container.remove()
                self.logger.info("Container stopped and removed successfully.")

            self.container = None
            return True

        except docker.errors.NotFound:
            self.logger.error(f"Container '{self.container_name}' not found.")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return False

    def restart(self):
        """Restart the container"""
        self.stop()
        time.sleep(2)
        return self.start_with_docker_sdk()

    def is_running(self):
        """Check if container is running"""
        try:
            if self.container and self.client:
                self.container.reload()
                return self.container.status == "running"
        except Exception as e:
            del e  # so ruff wont yell at me, we dont care what the reason is so del
            return False

    def get_logs(self, tail=100):
        """Get container logs"""
        try:
            if self.container and self.client:
                return self.container.logs(tail=tail).decode("utf-8")
        except Exception as e:
            self.logger.error(f"Error getting logs: {e}")
            return None

    def wait_for_db(self, timeout=60):
        """Wait for SQL Server to be ready to accept connections"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                cmd = f"/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P '{self.password}' -Q 'SELECT 1' -C"
                exit_code, _ = self.container.exec_run(cmd)
                if exit_code == 0:
                    return True
            except Exception:
                pass
            time.sleep(1)
        return False

    def copy_file_to_container(self, local_file, container_path):
        """Copy a file from the host to the container"""
        try:
            with open(local_file, "rb") as f:
                file_data = f.read()

            tar_stream = io.BytesIO()
            with tarfile.open(fileobj=tar_stream, mode="w") as tar:
                tarinfo = tarfile.TarInfo(name=os.path.basename(container_path))
                tarinfo.size = len(file_data)
                tarinfo.mtime = time.time()
                tar.addfile(tarinfo, io.BytesIO(file_data))

            tar_stream.seek(0)

            success = self.container.put_archive(
                os.path.dirname(container_path), tar_stream
            )

            return success
        except Exception as e:
            self.logger.error(f"Error copying file to container: {e}")
            return False

    def copy_file_from_container(self, container_path, local_file):
        """Copy a file from the container to the host"""
        try:
            bits, stat = self.container.get_archive(container_path)

            file_data = io.BytesIO()
            for chunk in bits:
                file_data.write(chunk)
            file_data.seek(0)

            with tarfile.open(fileobj=file_data) as tar:
                member = tar.getmembers()[0]
                f = tar.extractfile(member)
                with open(local_file, "wb") as outfile:
                    outfile.write(f.read())

            return True
        except Exception as e:
            self.logger.error(f"Error copying file from container: {e}")
            return False

    def execute_sql_file(self, sql_file):
        """Execute a SQL file within the container"""
        try:
            container_sql_path = f"/tmp/{os.path.basename(sql_file)}"
            if not self.copy_file_to_container(sql_file, container_sql_path):
                return False

            cmd = f"/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P '{self.password}' -i '{container_sql_path}' -C"
            exit_code, output = self.container.exec_run(cmd, tty=True)

            if exit_code == 0:
                self.logger.info(f"SQL file '{sql_file}' executed successfully")
                return True
            else:
                self.logger.error(
                    f"Failed to execute SQL file: {output.decode('utf-8')}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Error executing SQL file: {e}")
            return False

    def export_database(self, database_name, output_file):
        """Export specific database objects"""
        try:
            if not self.container:
                self.logger.error("Container is not running")
                return False

            export_script = f"""
USE [{database_name}];
SET NOCOUNT ON;

-- Database creation
PRINT 'CREATE DATABASE [{database_name}];'
PRINT 'GO'
PRINT 'USE [{database_name}];'
PRINT 'GO'
PRINT ''

-- Schemas
PRINT '-- SCHEMAS'
DECLARE @schemas NVARCHAR(MAX) = N'';
SELECT @schemas = @schemas + 
    'CREATE SCHEMA ' + QUOTENAME(name) + ';' + CHAR(13) + 'GO' + CHAR(13)
FROM sys.schemas
WHERE schema_id > 4 AND schema_id < 16384  -- User schemas only
ORDER BY name;
IF LEN(@schemas) > 0
    PRINT @schemas;
PRINT ''

-- Tables
PRINT '-- TABLES'
DECLARE @tables NVARCHAR(MAX) = N'';
SELECT @tables = @tables + 
    'CREATE TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ' (' + CHAR(13) +
    STUFF((
        SELECT ',' + CHAR(13) + '    ' + QUOTENAME(c.name) + ' ' + 
               UPPER(TYPE_NAME(c.user_type_id)) + 
               CASE WHEN TYPE_NAME(c.user_type_id) IN ('varchar', 'char', 'nvarchar', 'nchar') 
                    THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END + ')'
                    WHEN TYPE_NAME(c.user_type_id) IN ('decimal', 'numeric')
                    THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                    ELSE '' END +
               CASE WHEN c.is_nullable = 0 THEN ' NOT NULL' ELSE ' NULL' END +
               CASE WHEN c.is_identity = 1 THEN ' IDENTITY(' + 
                    CAST(IDENT_SEED(QUOTENAME(s.name) + '.' + QUOTENAME(t.name)) AS VARCHAR) + ',' + 
                    CAST(IDENT_INCR(QUOTENAME(s.name) + '.' + QUOTENAME(t.name)) AS VARCHAR) + ')' 
                    ELSE '' END
        FROM sys.columns c
        WHERE c.object_id = t.object_id
        ORDER BY c.column_id
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') + CHAR(13) + ');' + CHAR(13) + 'GO' + CHAR(13) + CHAR(13)
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.type = 'U'
ORDER BY s.name, t.name;
PRINT @tables;
PRINT ''

-- Views
PRINT '-- VIEWS'
DECLARE @views NVARCHAR(MAX) = N'';
SELECT @views = @views + 
    'CREATE VIEW ' + QUOTENAME(s.name) + '.' + QUOTENAME(v.name) + ' AS' + CHAR(13) +
    m.definition + CHAR(13) + 'GO' + CHAR(13) + CHAR(13)
FROM sys.views v
INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
ORDER BY s.name, v.name;
IF LEN(@views) > 0
    PRINT @views;
PRINT ''

-- Stored Procedures
PRINT '-- STORED PROCEDURES'
DECLARE @procs NVARCHAR(MAX) = N'';
SELECT @procs = @procs + 
    'CREATE PROCEDURE ' + QUOTENAME(s.name) + '.' + QUOTENAME(p.name) + CHAR(13) +
    m.definition + CHAR(13) + 'GO' + CHAR(13) + CHAR(13)
FROM sys.procedures p
INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
INNER JOIN sys.sql_modules m ON p.object_id = m.object_id
WHERE p.type = 'P' AND p.is_ms_shipped = 0
ORDER BY s.name, p.name;
IF LEN(@procs) > 0
    PRINT @procs;
PRINT ''

-- User-Defined Functions
PRINT '-- USER-DEFINED FUNCTIONS'
DECLARE @funcs NVARCHAR(MAX) = N'';
SELECT @funcs = @funcs + 
    'CREATE FUNCTION ' + QUOTENAME(s.name) + '.' + QUOTENAME(o.name) + CHAR(13) +
    m.definition + CHAR(13) + 'GO' + CHAR(13) + CHAR(13)
FROM sys.objects o
INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
INNER JOIN sys.sql_modules m ON o.object_id = m.object_id
WHERE o.type IN ('FN', 'IF', 'TF') AND o.is_ms_shipped = 0
ORDER BY s.name, o.name;
IF LEN(@funcs) > 0
    PRINT @funcs;
PRINT ''
"""

            temp_script_file = "temp_export_script.sql"
            with open(temp_script_file, "w", encoding="utf-8") as f:
                f.write(export_script)

            container_script_path = "/tmp/export_script.sql"
            if not self.copy_file_to_container(temp_script_file, container_script_path):
                return False

            container_output_path = "/tmp/export_output.sql"
            cmd = f"/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P '{self.password}' -d {database_name} -i '{container_script_path}' -o '{container_output_path}' -C"
            exit_code, output = self.container.exec_run(cmd, tty=True)

            if exit_code == 0:
                if self.copy_file_from_container(container_output_path, output_file):
                    self.logger.info(f"Database exported successfully to {output_file}")
                    return True
                else:
                    self.logger.error("Failed to copy export file from container")
                    return False
            else:
                self.logger.error(f"Export failed: {output.decode('utf-8')}")
                return False

        except Exception as e:
            self.logger.error(f"Error exporting database: {e}")
            return False
        finally:
            if os.path.exists(temp_script_file):
                os.remove(temp_script_file)

    def import_database(self, database_name, sql_file):
        """Import a database from a SQL script file"""
        try:
            create_db_cmd = f"/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P '{self.password}' -Q \"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{database_name}') CREATE DATABASE [{database_name}]\" -C"
            self.container.exec_run(create_db_cmd)

            container_sql_path = f"/tmp/{os.path.basename(sql_file)}"
            if not self.copy_file_to_container(sql_file, container_sql_path):
                return False

            import_cmd = f"/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P '{self.password}' -d {database_name} -i '{container_sql_path}' -C"
            exit_code, output = self.container.exec_run(import_cmd, tty=True)

            if exit_code == 0:
                self.logger.info(f"Database imported successfully from {sql_file}")
                return True
            else:
                self.logger.error(f"Import failed: {output.decode('utf-8')}")
                return False

        except Exception as e:
            self.logger.error(f"Error importing database: {e}")
            return False

    def cleanup(self):
        """Cleanup method called on exit"""
        if self.is_running():
            self.logger.info("Cleaning up SQL Server container...")
            self.stop()

    def __enter__(self):
        """Context manager support"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support"""
        self.stop()


class MqttContainer:
    def __init__(
        self,
        port=1883,
        websocket_port=9001,
        container_name="mosquitto-mqtt",
        image="eclipse-mosquitto:latest",
        config_file=None,
        persistence_location=None,
    ):
        self.port = port
        self.websocket_port = websocket_port
        self.container_name = container_name
        self.image = image
        self.config_file = config_file
        self.persistence_location = persistence_location
        self.container = None
        self.client = None

        self.logger = logging.getLogger(__name__)

        atexit.register(self.cleanup)

    def start(self):
        """Start the MQTT container"""
        try:
            if not self.client:
                self.client = docker.from_env()

            try:
                existing = self.client.containers.get(self.container_name)
                if existing.status == "running":
                    self.logger.info(
                        f"Container '{self.container_name}' is already running."
                    )
                    self.container = existing
                    return True
                else:
                    self.logger.info(
                        f"Removing existing container '{self.container_name}'..."
                    )
                    existing.remove(force=True)
            except docker.errors.NotFound:
                pass

            self.logger.info(f"Starting MQTT container '{self.container_name}'...")

            container_config = {
                "image": self.image,
                "name": self.container_name,
                "detach": True,
                "ports": {"1883/tcp": self.port, "9001/tcp": self.websocket_port},
            }

            self.container = self.client.containers.run(**container_config)

            self.logger.info(f"Container started with ID: {self.container.id[:12]}")

            self.logger.info("Waiting for MQTT broker to be ready...")
            time.sleep(3)

            self.container.reload()
            if self.container.status == "running":
                self.logger.info("MQTT container is running successfully!")
                self.logger.info(
                    f"MQTT port: {self.port}, WebSocket port: {self.websocket_port}"
                )
                return True
            else:
                self.logger.error(f"Container status: {self.container.status}")
                return False

        except docker.errors.APIError as e:
            self.logger.error(f"Docker API error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return False

    def stop(self):
        """Stop the container"""
        try:
            if not self.container and self.client:
                try:
                    self.container = self.client.containers.get(self.container_name)
                except docker.errors.NotFound:
                    self.logger.warning(f"Container '{self.container_name}' not found.")
                    return False

            if self.container:
                self.logger.info(f"Stopping container '{self.container_name}'...")
                self.container.stop()
                self.container.remove()
                self.logger.info("Container stopped and removed successfully.")
                self.container = None
                return True
            else:
                self.logger.warning("No container instance found to stop.")
                return False

        except docker.errors.APIError as e:
            self.logger.error(f"Docker API error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return False

    def is_running(self):
        """Check if container is running"""
        try:
            if self.container:
                self.container.reload()
                return self.container.status == "running"
            elif self.client:
                try:
                    container = self.client.containers.get(self.container_name)
                    return container.status == "running"
                except docker.errors.NotFound:
                    return False
            return False
        except Exception as e:
            self.logger.error(f"Error checking status: {e}")
            return False

    def get_logs(self, tail=100):
        """Get container logs"""
        try:
            if not self.container and self.client:
                self.container = self.client.containers.get(self.container_name)

            if self.container:
                return self.container.logs(tail=tail).decode("utf-8")
            else:
                return None
        except Exception as e:
            self.logger.error(f"Error getting logs: {e}")
            return None

    def pull_image(self):
        """Pull the MQTT image"""
        try:
            if not self.client:
                self.client = docker.from_env()

            self.logger.info(f"Pulling image: {self.image}")
            self.client.images.pull(self.image)
            self.logger.info("Image pulled successfully.")
            return True
        except docker.errors.APIError as e:
            self.logger.error(f"Docker API error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return False

    def cleanup(self):
        """Cleanup method called on exit"""
        if self.is_running():
            self.logger.info("Cleaning up MQTT container...")
            self.stop()

    def __enter__(self):
        """Context manager support"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support"""
        self.stop()
