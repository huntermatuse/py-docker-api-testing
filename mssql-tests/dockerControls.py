import docker
import time
import atexit


class SQLServerContainer:
    def __init__(self, password="YourStrong%Passw0rd", port=1433, container_name="testing-mssql-container"):
        self.password = password
        self.port = port
        self.container_name = container_name
        self.container = None
        self.client = None
        
        atexit.register(self.cleanup)
    
    def start_with_docker_sdk(self):
        """Start container using Docker SDK (preferred method)"""
        try:
            if not self.client:
                self.client = docker.from_env()
            
            try:
                existing = self.client.containers.get(self.container_name)
                if existing.status == "running":
                    print(f"Container '{self.container_name}' is already running.")
                    self.container = existing
                    return True
                else:
                    print(f"Removing existing container '{self.container_name}'...")
                    existing.remove(force=True)
            except docker.errors.NotFound:
                pass
            
            print(f"Starting SQL Server container '{self.container_name}'...")
            
            self.container = self.client.containers.run(
                "mcr.microsoft.com/mssql/server:2019-latest",
                detach=True,
                name=self.container_name,
                environment={
                    "ACCEPT_EULA": "Y",
                    "SA_PASSWORD": self.password
                },
                ports={"1433/tcp": self.port}
            )
            
            print(f"Container started with ID: {self.container.id[:12]}")
            
            print("Waiting for SQL Server to be ready...")
            time.sleep(10)
            
            self.container.reload()
            if self.container.status == "running":
                print("SQL Server container is running successfully!")
                return True
            else:
                print(f"Container status: {self.container.status}")
                return False
                
        except docker.errors.APIError as e:
            print(f"Docker API error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False
    
    def stop(self):
        """Stop the container"""
        try:
            if self.container and self.client:
                print(f"Stopping container '{self.container_name}'...")
                self.container.stop()
                self.container.remove()
                print("Container stopped and removed successfully.")
            
            self.container = None
            return True
    
        except docker.errors.NotFound:
            print(f"Container '{self.container_name}' not found.")
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
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
        except:
            return False
    
    def get_logs(self, tail=100):
        """Get container logs"""
        try:
            if self.container and self.client:
                return self.container.logs(tail=tail).decode('utf-8')
        except Exception as e:
            print(f"Error getting logs: {e}")
            return None
    
    def cleanup(self):
        """Cleanup method called on exit"""
        if self.is_running():
            print("Cleaning up SQL Server container...")
            self.stop()
    
    def __enter__(self):
        """Context manager support"""
        self.start_with_docker_sdk()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support"""
        self.stop()


class MqttContainer:
    pass