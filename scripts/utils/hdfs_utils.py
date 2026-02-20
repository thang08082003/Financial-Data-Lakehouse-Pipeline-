"""
HDFS Utilities
Các hàm tiện ích để làm việc với Hadoop HDFS
"""

import os
import json
import logging
from typing import List, Optional, Dict
from datetime import datetime
from hdfs import InsecureClient
from hdfs.util import HdfsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HDFSManager:
    """
    Manager class để tương tác với HDFS
    """
    
    def __init__(
        self,
        namenode_url: str = 'http://namenode:9870',
        user: str = 'root'
    ):
        """
        Khởi tạo HDFS manager
        
        Args:
            namenode_url: URL của HDFS NameNode
            user: HDFS username
        """
        self.namenode_url = namenode_url
        self.user = user
        self.client = InsecureClient(namenode_url, user=user)
        logger.info(f"Connected to HDFS at {namenode_url}")
    
    def create_directory(self, path: str) -> bool:
        """
        Tạo directory trong HDFS
        
        Args:
            path: HDFS path
            
        Returns:
            True nếu tạo thành công
        """
        try:
            self.client.makedirs(path)
            logger.info(f"Created directory: {path}")
            return True
        except HdfsError as e:
            if 'already exists' in str(e).lower():
                logger.info(f"Directory already exists: {path}")
                return True
            logger.error(f"Error creating directory {path}: {str(e)}")
            return False
    
    def upload_file(
        self,
        local_path: str,
        hdfs_path: str,
        overwrite: bool = True
    ) -> bool:
        """
        Upload file lên HDFS
        
        Args:
            local_path: Đường dẫn file local
            hdfs_path: Đường dẫn HDFS đích
            overwrite: Có ghi đè file nếu đã tồn tại không
            
        Returns:
            True nếu upload thành công
        """
        try:
            # Tạo directory nếu chưa tồn tại
            hdfs_dir = os.path.dirname(hdfs_path)
            self.create_directory(hdfs_dir)
            
            # Upload file
            self.client.upload(hdfs_path, local_path, overwrite=overwrite)
            logger.info(f"Uploaded {local_path} to {hdfs_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            return False
    
    def upload_json_data(
        self,
        data: Dict,
        hdfs_path: str,
        overwrite: bool = True
    ) -> bool:
        """
        Upload JSON data trực tiếp lên HDFS
        
        Args:
            data: Dictionary data để upload
            hdfs_path: Đường dẫn HDFS đích
            overwrite: Có ghi đè file nếu đã tồn tại không
            
        Returns:
            True nếu upload thành công
        """
        try:
            # Tạo directory nếu chưa tồn tại
            hdfs_dir = os.path.dirname(hdfs_path)
            self.create_directory(hdfs_dir)
            
            # Convert to JSON string
            json_str = json.dumps(data, indent=2)
            
            # Write to HDFS
            with self.client.write(hdfs_path, encoding='utf-8', overwrite=overwrite) as writer:
                writer.write(json_str)
            
            logger.info(f"Uploaded JSON data to {hdfs_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading JSON data: {str(e)}")
            return False
    
    def download_file(
        self,
        hdfs_path: str,
        local_path: str,
        overwrite: bool = True
    ) -> bool:
        """
        Download file từ HDFS
        
        Args:
            hdfs_path: Đường dẫn HDFS source
            local_path: Đường dẫn local đích
            overwrite: Có ghi đè file nếu đã tồn tại không
            
        Returns:
            True nếu download thành công
        """
        try:
            # Tạo local directory nếu chưa tồn tại
            local_dir = os.path.dirname(local_path)
            os.makedirs(local_dir, exist_ok=True)
            
            # Download file
            self.client.download(hdfs_path, local_path, overwrite=overwrite)
            logger.info(f"Downloaded {hdfs_path} to {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading file: {str(e)}")
            return False
    
    def read_json(self, hdfs_path: str) -> Optional[Dict]:
        """
        Đọc JSON file từ HDFS
        
        Args:
            hdfs_path: Đường dẫn HDFS file
            
        Returns:
            Dictionary data hoặc None nếu lỗi
        """
        try:
            with self.client.read(hdfs_path, encoding='utf-8') as reader:
                content = reader.read()
                data = json.loads(content)
            
            logger.info(f"Read JSON from {hdfs_path}")
            return data
            
        except Exception as e:
            logger.error(f"Error reading JSON: {str(e)}")
            return None
    
    def list_directory(self, path: str) -> List[str]:
        """
        List các files/directories trong HDFS path
        
        Args:
            path: HDFS path
            
        Returns:
            List các file/directory names
        """
        try:
            files = self.client.list(path)
            logger.info(f"Found {len(files)} items in {path}")
            return files
        except Exception as e:
            logger.error(f"Error listing directory: {str(e)}")
            return []
    
    def delete(self, path: str, recursive: bool = False) -> bool:
        """
        Xóa file hoặc directory trong HDFS
        
        Args:
            path: HDFS path
            recursive: Xóa recursive nếu là directory
            
        Returns:
            True nếu xóa thành công
        """
        try:
            self.client.delete(path, recursive=recursive)
            logger.info(f"Deleted {path}")
            return True
        except Exception as e:
            logger.error(f"Error deleting: {str(e)}")
            return False
    
    def exists(self, path: str) -> bool:
        """
        Kiểm tra xem path có tồn tại trong HDFS không
        
        Args:
            path: HDFS path
            
        Returns:
            True nếu path tồn tại
        """
        try:
            status = self.client.status(path, strict=False)
            return status is not None
        except Exception:
            return False
    
    def get_file_info(self, path: str) -> Optional[Dict]:
        """
        Lấy thông tin về file/directory
        
        Args:
            path: HDFS path
            
        Returns:
            Dictionary chứa thông tin hoặc None
        """
        try:
            info = self.client.status(path)
            return info
        except Exception as e:
            logger.error(f"Error getting file info: {str(e)}")
            return None
    
    def create_partitioned_path(
        self,
        base_path: str,
        date: Optional[datetime] = None,
        partition_by: str = 'date'
    ) -> str:
        """
        Tạo partitioned path theo date
        
        Args:
            base_path: Base HDFS path
            date: Date để partition (default: today)
            partition_by: Partition strategy ('date', 'year-month', 'year')
            
        Returns:
            Partitioned path
        """
        if date is None:
            date = datetime.now()
        
        if partition_by == 'date':
            # /base_path/year=2024/month=01/day=15
            path = f"{base_path}/year={date.year}/month={date.month:02d}/day={date.day:02d}"
        elif partition_by == 'year-month':
            # /base_path/year=2024/month=01
            path = f"{base_path}/year={date.year}/month={date.month:02d}"
        elif partition_by == 'year':
            # /base_path/year=2024
            path = f"{base_path}/year={date.year}"
        else:
            raise ValueError(f"Unsupported partition_by: {partition_by}")
        
        return path
    
    def upload_with_partitioning(
        self,
        local_path: str,
        base_hdfs_path: str,
        filename: str,
        date: Optional[datetime] = None,
        partition_by: str = 'date'
    ) -> Optional[str]:
        """
        Upload file với partitioning
        
        Args:
            local_path: Đường dẫn file local
            base_hdfs_path: Base HDFS path
            filename: Tên file trong HDFS
            date: Date để partition
            partition_by: Partition strategy
            
        Returns:
            Full HDFS path nếu thành công, None nếu failed
        """
        try:
            # Tạo partitioned path
            partitioned_path = self.create_partitioned_path(
                base_hdfs_path,
                date,
                partition_by
            )
            
            # Full path including filename
            full_path = f"{partitioned_path}/{filename}"
            
            # Upload
            if self.upload_file(local_path, full_path):
                return full_path
            return None
            
        except Exception as e:
            logger.error(f"Error uploading with partitioning: {str(e)}")
            return None
    
    def get_hdfs_size(self, path: str) -> Optional[int]:
        """
        Lấy size của file hoặc directory (bytes)
        
        Args:
            path: HDFS path
            
        Returns:
            Size in bytes hoặc None nếu lỗi
        """
        try:
            info = self.client.content(path)
            return info.get('length', 0)
        except Exception as e:
            logger.error(f"Error getting size: {str(e)}")
            return None


def main():
    """
    Test HDFS utilities
    """
    hdfs_manager = HDFSManager()
    
    # Test create directory
    hdfs_manager.create_directory('/data/test')
    
    # Test upload JSON
    test_data = {
        'test': 'data',
        'timestamp': datetime.now().isoformat()
    }
    hdfs_manager.upload_json_data(test_data, '/data/test/test.json')
    
    # Test read JSON
    read_data = hdfs_manager.read_json('/data/test/test.json')
    print(f"Read data: {read_data}")
    
    # Test list directory
    files = hdfs_manager.list_directory('/data/test')
    print(f"Files in /data/test: {files}")


if __name__ == '__main__':
    main()
