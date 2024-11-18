from flask import Flask, request, jsonify
import ctypes
import json
import time
import uuid
import sys
import os
import signal
import atexit
from datetime import datetime
from typing import Optional
from dataclasses import dataclass
from ctypes import c_int, c_char_p, c_void_p, c_size_t, CDLL
from fluent import sender
import threading
from werkzeug.serving import make_server


class ServiceExit(Exception):
    pass


app = Flask(__name__)

shutdown_flag = threading.Event()

@dataclass
class CacheStats:
    total_size: int
    used_size: int
    total_entries: int
    hits: int
    misses: int

class FlaskServer:
    def __init__(self, app, host='0.0.0.0', port=4001):
        self.server = make_server(host, port, app)
        self.server_thread = threading.Thread(target=self.server.serve_forever)

    def start(self):
        self.server_thread.start()

    def shutdown(self):
        self.server.shutdown()
        self.server_thread.join()

class CacheWriter:
    def __init__(self):
        fluent_host = os.getenv('FLUENT_HOST', 'localhost')
        fluent_port = int(os.getenv('FLUENT_PORT', '24224'))
        print(f"Connecting to fluentd at {fluent_host}:{fluent_port}")
        
        self.logger = sender.FluentSender(
            'cache',
            host=fluent_host,
            port=fluent_port,
            nanosecond_precision=True
        )
        self.node_id = "Writer_Service"
        self.service_name = "CacheWriterService"
        self.send_registration()
        self.init_cache()
        self.running = True
        self.shutdown_event = threading.Event()
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop)
        self.heartbeat_thread.daemon = False
        self.heartbeat_thread.start()

    def send_registration(self):
        """Send service registration message"""
        registration_msg = {
            "message_type": "REGISTRATION",
            "node_id": self.node_id,
            "service_name": self.service_name,
            "status": "UP",
            "timestamp": datetime.now().isoformat()
        }
        if not self.logger.emit('registration', registration_msg):
            print(f"Failed to send registration: {self.logger.last_error}")

    def send_heartbeat(self):
        """Send single heartbeat message"""
        heartbeat_msg = {
            "node_id": self.node_id,
            "message_type": "HEARTBEAT",
            "status": "UP",
            "timestamp": datetime.now().isoformat()
        }
        if not self.logger.emit('heartbeat', heartbeat_msg):
            print(f"Failed to send heartbeat: {self.logger.last_error}")

    def heartbeat_loop(self):
        """Continuous heartbeat sender"""
        while self.running and not self.shutdown_event.is_set():
            self.send_heartbeat()
            # Use event with timeout instead of sleep for better shutdown response
            self.shutdown_event.wait(timeout=5)

    def log_info(self, message: str, **kwargs):
        """Send INFO level log message"""
        try:
            log_msg = {
                "log_id": str(uuid.uuid4()),
                "node_id": self.node_id,
                "log_level": "INFO",
                "message_type": "LOG",
                "message": message,
                "service_name": self.service_name,
                "timestamp": datetime.now().isoformat()
            }
            log_msg.update(kwargs)
            if not self.logger.emit('log.info', log_msg):
                print(f"Failed to send info log: {self.logger.last_error}")
        except Exception as e:
            print(f"Error sending info log: {str(e)}")

    def log_warn(self, message: str, response_time_ms: float, threshold_limit_ms: float):
        """Send WARN level log message"""
        try:
            log_msg = {
                "log_id": str(uuid.uuid4()),
                "node_id": self.node_id,
                "log_level": "WARN",
                "message_type": "LOG",
                "message": message,
                "service_name": self.service_name,
                "response_time_ms": response_time_ms,
                "threshold_limit_ms": threshold_limit_ms,
                "timestamp": datetime.now().isoformat()
            }
            if not self.logger.emit('log.warn', log_msg):
                print(f"Failed to send warn log: {self.logger.last_error}")
        except Exception as e:
            print(f"Error sending warn log: {str(e)}")

    def log_error(self, message: str, error_code: str, error_message: str):
        """Send ERROR level log message"""
        try:
            log_msg = {
                "log_id": str(uuid.uuid4()),
                "node_id": self.node_id,
                "log_level": "ERROR",
                "message_type": "LOG",
                "message": message,
                "service_name": self.service_name,
                "error_details": {
                    "error_code": error_code,
                    "error_message": error_message
                },
                "timestamp": datetime.now().isoformat()
            }
            if not self.logger.emit('log.error', log_msg):
                print(f"Failed to send error log: {self.logger.last_error}")
        except Exception as e:
            print(f"Error sending error log: {str(e)}")

    def init_cache(self):
        """Initialize cache connection"""
        try:
            self.lib = CDLL("/app/libcache.so")
            
            # Set up function signatures
            self.lib.cache_connect.restype = c_int
            self.lib.cache_connect.argtypes = []
            
            self.lib.cache_set.restype = c_int
            self.lib.cache_set.argtypes = [c_char_p, c_void_p, c_size_t]
            
            self.lib.cache_delete.restype = c_int
            self.lib.cache_delete.argtypes = [c_char_p]
            
            # Connect to cache
            result = self.lib.cache_connect()
            if result != 0:
                self.log_error(
                    "Cache connection failed", 
                    "CONN_ERROR", 
                    "Failed to establish connection with cache"
                )
                raise RuntimeError("Failed to connect to cache")
            
            self.log_info("Cache connection established successfully")
            
        except Exception as e:
            self.log_error(
                "Cache initialization failed",
                "INIT_ERROR",
                str(e)
            )
            raise

    def set(self, key: str, value: str) -> bool:
        """Set value in cache"""
        start_time = time.time()
        try:
            key_bytes = key.encode('utf-8')
            value_bytes = value.encode('utf-8')
            result = self.lib.cache_set(
                key_bytes,
                ctypes.cast(value_bytes, c_void_p),
                len(value_bytes)
            )
            
            response_time = (time.time() - start_time) * 1000
            
            if result == 0:
                self.log_info(
                    f"Set value for key: {key}",
                    operation="SET",
                    key=key,
                    value_size=len(value_bytes)
                )
                
                if response_time > 100:
                    self.log_warn(
                        f"Slow SET operation for key: {key}",
                        response_time,
                        100.0
                    )
                
                return True
            else:
                self.log_error(
                    f"Failed to set value for key: {key}",
                    "SET_ERROR",
                    "Cache set operation returned error"
                )
                return False
                
        except Exception as e:
            self.log_error(
                f"Exception during SET operation for key: {key}",
                "SET_EXCEPTION",
                str(e)
            )
            return False

    def delete(self, key: str) -> bool:
        """Delete value from cache"""
        start_time = time.time()
        try:
            key_bytes = key.encode('utf-8')
            result = self.lib.cache_delete(key_bytes)
            
            response_time = (time.time() - start_time) * 1000
            
            if result == 0:
                self.log_info(
                    f"Deleted key: {key}",
                    operation="DELETE",
                    key=key
                )
                
                if response_time > 100:
                    self.log_warn(
                        f"Slow DELETE operation for key: {key}",
                        response_time,
                        100.0
                    )
                
                return True
            else:
                self.log_error(
                    f"Failed to delete key: {key}",
                    "DELETE_ERROR",
                    "Cache delete operation returned error"
                )
                return False
                
        except Exception as e:
            self.log_error(
                f"Exception during DELETE operation for key: {key}",
                "DELETE_EXCEPTION",
                str(e)
            )
            return False

    def cleanup(self):
        """Cleanup before exit"""
        if hasattr(self, 'running') and self.running:
            print("Starting cleanup process...")
            self.running = False
            self.shutdown_event.set()
            
            try:
                # Stop heartbeat thread first
                if hasattr(self, 'heartbeat_thread') and self.heartbeat_thread.is_alive():
                    print("Stopping heartbeat thread...")
                    self.heartbeat_thread.join(timeout=5)

                messages = [
                    ('log.warn', {
                        "log_id": str(uuid.uuid4()),
                        "node_id": self.node_id,
                        "log_level": "WARN",
                        "message_type": "LOG",
                        "message": "node going off",
                        "service_name": self.service_name,
                        "response_time_ms": "0",
                        "threshold_limit_ms": "0",
                        "timestamp": datetime.now().isoformat()
                    }),
                    ('registration', {
                        "message_type": "REGISTRATION",
                        "node_id": self.node_id,
                        "service_name": self.service_name,
                        "status": "DOWN",
                        "timestamp": datetime.now().isoformat()
                    }),
                    ('heartbeat', {
                        "node_id": self.node_id,
                        "message_type": "HEARTBEAT",
                        "status": "DOWN",
                        "timestamp": datetime.now().isoformat()
                    })
                ]

                for tag, msg in messages:
                    print(f"Sending {tag} message...")
                    success = self.logger.emit(tag, msg)
                    if not success:
                        print(f"Failed to send {tag}: {self.logger.last_error}")
                    time.sleep(0.5)

                # Final wait for messages to be sent
                print("Waiting for final messages to be processed...")
                time.sleep(2)

            except Exception as e:
                print(f"Error during cleanup: {str(e)}")
            finally:
                try:
                    if hasattr(self, 'logger'):
                        print("Closing logger...")
                        self.logger.close()
                        print("Logger closed")
                except Exception as e:
                    print(f"Error closing logger: {str(e)}")
                print("Cleanup completed")

@app.route('/set', methods=['POST'])
def set_value():
    data = request.get_json()
    key = data.get('key')
    value = data.get('value')
    
    if not key or not value:
        return jsonify({'error': 'Missing key or value'}), 400
    
    cache = app.config['cache']
    success = cache.set(key, value)
    
    if success:
        return jsonify({'message': 'Value set successfully'})
    return jsonify({'error': 'Failed to set value'}), 500

@app.route('/delete', methods=['DELETE'])
def delete_value():
    data = request.get_json()
    key = data.get('key')
    
    if not key:
        return jsonify({'error': 'Missing key'}), 400
    
    cache = app.config['cache']
    success = cache.delete(key)
    
    if success:
        return jsonify({'message': 'Value deleted successfully'})
    return jsonify({'error': 'Failed to delete value'}), 500

def shutdown_handler(signum, frame):
    print(f"\nCaught signal {signum}")
    if hasattr(app, 'flask_server'):
        print("Shutting down Flask server...")
        app.flask_server.shutdown()
    if 'cache' in app.config:
        print("Starting graceful shutdown...")
        app.config['cache'].cleanup()
    print("Shutdown complete")
    sys.exit(0)

def register_shutdown_handlers():
    # Register for different signals
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    atexit.register(lambda: app.config['cache'].cleanup() if 'cache' in app.config else None)

def signal_handler(signum, frame):
    print(f"\nReceived signal {signum}")
    shutdown_flag.set()

def flask_thread():
    app.run(host='0.0.0.0', port=4001, use_reloader=False)

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        cache = CacheWriter()
        app.config['cache'] = cache

        server_thread = threading.Thread(target=flask_thread)
        server_thread.daemon = True
        server_thread.start()
        
        print("Service started, waiting for requests...")
        
        while not shutdown_flag.is_set():
            time.sleep(0.1)
            
        print("Shutdown flag received, starting cleanup...")
        if 'cache' in app.config:
            app.config['cache'].cleanup()
            
    except Exception as e:
        print(f"Error during execution: {e}")
    finally:
        if 'cache' in app.config:
            app.config['cache'].cleanup()
        print("Service stopped")
