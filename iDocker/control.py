import docker
import time
import atexit
import logging


class SQLServerContainer:
    def __init__(self, password="YourStrong%Passw0rd", port=1433, container_name="testing-mssql-container"):
        self.password = password
        self.port = port
        self.container_name = container_name
        self.container = None
        self.client = None

        self.logger = logging.getLogger(__name__)
        
        atexit.register(self.cleanup)
    
    def start_with_docker_sdk(self):
        """Start container using Docker SDK (preferred method)"""
        try:
            if not self.client:
                self.client = docker.from_env()
            
            try:
                existing = self.client.containers.get(self.container_name)
                if existing.status == "running":
                    self.logger.info(f"Container '{self.container_name}' is already running.")
                    self.container = existing
                    return True
                else:
                    self.logger.info(f"Removing existing container '{self.container_name}'...")
                    existing.remove(force=True)
            except docker.errors.NotFound:
                pass
            
            self.logger.info(f"Starting SQL Server container '{self.container_name}'...")
            
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
        except:
            return False
    
    def get_logs(self, tail=100):
        """Get container logs"""
        try:
            if self.container and self.client:
                return self.container.logs(tail=tail).decode('utf-8')
        except Exception as e:
            self.logger.error(f"Error getting logs: {e}")
            return None
    
    def cleanup(self):
        """Cleanup method called on exit"""
        if self.is_running():
            self.logger.info("Cleaning up SQL Server container...")
            self.stop()
    
    def __enter__(self):
        """Context manager support"""
        self.start_with_docker_sdk()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support"""
        self.stop()


class MqttContainer:
    def __init__(self, 
                 port=1883, 
                 websocket_port=9001, 
                 container_name="mosquitto-mqtt",
                 image="eclipse-mosquitto:latest",
                 config_file=None,
                 persistence_location=None):
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
                    self.logger.info(f"Container '{self.container_name}' is already running.")
                    self.container = existing
                    return True
                else:
                    self.logger.info(f"Removing existing container '{self.container_name}'...")
                    existing.remove(force=True)
            except docker.errors.NotFound:
                pass
            
            self.logger.info(f"Starting MQTT container '{self.container_name}'...")
            
            container_config = {
                "image": self.image,
                "name": self.container_name,
                "detach": True,
                "ports": {
                    "1883/tcp": self.port,
                    "9001/tcp": self.websocket_port
                }
            }

            self.container = self.client.containers.run(**container_config)
                
            self.logger.info(f"Container started with ID: {self.container.id[:12]}")
            
            self.logger.info("Waiting for MQTT broker to be ready...")
            time.sleep(3)
            
            self.container.reload()
            if self.container.status == "running":
                self.logger.info("MQTT container is running successfully!")
                self.logger.info(f"MQTT port: {self.port}, WebSocket port: {self.websocket_port}")
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
                return self.container.logs(tail=tail).decode('utf-8')
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