"""
数据库API服务
提供RESTful API接口，用于访问内网数据库
"""

import os
import sys
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, Body, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

# 设置项目根目录（用于导入模块）
project_root = os.path.join(os.path.dirname(__file__), '..', '..')

# 设置配置目录环境变量
config_dir = os.path.join(project_root, 'config')
if os.path.exists(config_dir):
    os.environ['CONFIG_DIR'] = config_dir

# 设置数据目录环境变量
data_dir = os.path.join(project_root, 'datasets')
if not os.path.exists(data_dir):
    os.makedirs(data_dir, exist_ok=True)
os.environ['DATA_DIR'] = data_dir

# 添加项目路径到sys.path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 导入数据库和Cookie工具
try:
    # 优先尝试从已安装的包导入
    from utils.db_utils import DynamicDatabase
    from utils.qms_db import QMSDatabase
    from utils.CookieTool import CookieTools
    print("✓ 使用已安装的 midea_workspace 包")
except ImportError:
    try:
        # 尝试从源码导入
        from src.utils.db_utils import DynamicDatabase
        from src.utils.qms_db import QMSDatabase
        from src.utils.CookieTool import CookieTools
        print("✓ 使用源码模式")
    except ImportError as e:
        print(f"✗ 导入失败: {e}")
        print("请确保已安装 midea_workspace 包或在源码目录下运行")
        raise
# 创建FastAPI应用
app = FastAPI(
    title="数据库API服务",
    description="提供访问内网数据库的RESTful API接口",
    version="1.0.0"
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 在生产环境中应该设置具体的域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 数据库连接实例
db_utils = DynamicDatabase()
qms_db = QMSDatabase()

# 请求模型
class QueryRequest(BaseModel):
    database: str = Field(..., description="数据库名称")
    table: str = Field(..., description="表名")
    columns: Optional[List[str]] = Field(None, description="要查询的列，如果为空则查询所有列")
    where: Optional[str] = Field(None, description="WHERE条件")
    limit: Optional[int] = Field(100, description="返回记录数限制")
    offset: Optional[int] = Field(0, description="偏移量")

class SaveRequest(BaseModel):
    database: str = Field(..., description="数据库名称")
    table: str = Field(..., description="表名")
    data: List[Dict[str, Any]] = Field(..., description="要保存的数据")
    additional_fields: Optional[Dict[str, Any]] = Field(None, description="额外字段")
    check_duplicate: Optional[bool] = Field(False, description="是否检查重复数据")
    duplicate_check_fields: Optional[List[str]] = Field(None, description="用于检查重复的字段列表")

class QMSSaveRequest(BaseModel):
    table: str = Field(..., description="表名")
    org_id: str = Field(..., description="组织ID")
    data: List[Dict[str, Any]] = Field(..., description="要保存的数据")
    check_duplicate: Optional[bool] = Field(True, description="是否检查重复数据")
    duplicate_check_fields: Optional[List[str]] = Field(None, description="用于检查重复的字段列表")

class CookieRequest(BaseModel):
    platform: str = Field(..., description="平台名称 (qms, qbi, dataexplorer, datamix, doc等)")
    org_id: Optional[int] = Field(582, description="组织ID")
    user: Optional[str] = Field(None, description="用户名")

class MotorFailureAnalysisRequest(BaseModel):
    motor_model: str = Field(..., description="电机型号")
    motor_serial_no: str = Field(..., description="电机序列号")
    motor_detail_info: Optional[Dict[str, Any]] = Field(None, description="电机详细信息（JSON格式）")
    production_date: Optional[str] = Field(None, description="电机生产日期 (YYYY-MM-DD)")
    failure_report_time: str = Field(..., description="分析日期 (YYYY-MM-DD HH:MM:SS)")
    failure_occur_time: str = Field(..., description="失效发生时间 (YYYY-MM-DD HH:MM:SS)")
    failure_phenomenon: str = Field(..., description="失效现象描述")
    failure_type: str = Field(..., description="失效大类")
    failure_subtype: Optional[str] = Field("", description="失效细分类型")
    analysis_person: str = Field(..., description="分析人员")
    analysis_department: str = Field(..., description="分析部门")
    analysis_method: Optional[str] = Field(None, description="分析方法/手段")
    failure_cause: str = Field(..., description="失效根本原因")
    handling_measures: str = Field(..., description="处理措施/整改方案")
    analysis_status: Optional[str] = Field("待分析", description="分析状态")
    processing_result: Optional[str] = Field("", description="处理结果")
    remark: Optional[str] = Field(None, description="备注信息")
    creator: str = Field(..., description="录入人")
    ext1: Optional[str] = Field(None, description="扩展字段1")
    ext2: Optional[str] = Field(None, description="扩展字段2")
    ext3: Optional[str] = Field(None, description="扩展字段3")
class MotorFailureAnalysisUpdateRequest(BaseModel):
    id: int = Field(..., description="记录ID")
    motor_model: Optional[str] = Field(None, description="电机型号")
    motor_serial_no: Optional[str] = Field(None, description="电机序列号")
    motor_detail_info: Optional[Dict[str, Any]] = Field(None, description="电机详细信息（JSON格式）")
    production_date: Optional[str] = Field(None, description="电机生产日期 (YYYY-MM-DD)")
    failure_report_time: Optional[str] = Field(None, description="分析日期 (YYYY-MM-DD HH:MM:SS)")
    failure_occur_time: Optional[str] = Field(None, description="失效发生时间 (YYYY-MM-DD HH:MM:SS)")
    failure_phenomenon: Optional[str] = Field(None, description="失效现象描述")
    failure_type: Optional[str] = Field(None, description="失效大类")
    failure_subtype: Optional[str] = Field(None, description="失效细分类型")
    analysis_person: Optional[str] = Field(None, description="分析人员")
    analysis_department: Optional[str] = Field(None, description="分析部门")
    analysis_method: Optional[str] = Field(None, description="分析方法/手段")
    failure_cause: Optional[str] = Field(None, description="失效根本原因")
    handling_measures: Optional[str] = Field(None, description="处理措施/整改方案")
    analysis_status: Optional[str] = Field(None, description="分析状态")
    processing_result: Optional[str] = Field(None, description="处理结果")
    remark: Optional[str] = Field(None, description="备注信息")
    updater: str = Field(..., description="更新人")
    ext1: Optional[str] = Field(None, description="扩展字段1")
    ext2: Optional[str] = Field(None, description="扩展字段2")
    ext3: Optional[str] = Field(None, description="扩展字段3")

class MotorFailureAnalysisQueryRequest(BaseModel):
    id: Optional[int] = Field(None, description="记录ID")
    motor_model: Optional[str] = Field(None, description="电机型号")
    motor_serial_no: Optional[str] = Field(None, description="电机序列号")
    failure_type: Optional[str] = Field(None, description="失效大类")
    analysis_status: Optional[str] = Field(None, description="分析状态")
    analysis_person: Optional[str] = Field(None, description="分析人员")
    start_date: Optional[str] = Field(None, description="失效发生时间起始 (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="失效发生时间结束 (YYYY-MM-DD)")
    limit: Optional[int] = Field(100, description="返回记录数限制")
    offset: Optional[int] = Field(0, description="偏移量")
# 健康检查接口
@app.get("/health")
async def health_check():
    """健康检查接口"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "数据库API服务"
    }

# 数据库连接检查
@app.get("/api/db/check")
async def check_db_connection():
    """检查数据库连接状态"""
    try:
        # 获取数据库连接信息
        conn_info = db_utils.db.get_connection_info()
        print(f"API服务尝试连接数据库: {conn_info['host']}:{conn_info['port']}, 用户: {conn_info['username']}")
        
        # 测试连接
        test_result = db_utils.db.test_connection()
        
        if test_result["code"] == 0:
            return {
                "status": "success",
                "message": test_result["message"],
                "connection_info": conn_info,
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail=f"无法连接到数据库: {test_result['message']}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据库连接检查失败: {str(e)}")

# 获取数据库列表
@app.get("/api/db/databases")
async def get_databases():
    """获取所有数据库列表"""
    try:
        # 测试连接
        test_result = db_utils.db.test_connection()
        if test_result["code"] != 0:
            raise HTTPException(status_code=500, detail=f"无法连接到数据库: {test_result['message']}")
        
        # 执行查询获取数据库列表
        result = db_utils.db.execute_query("SHOW DATABASES")
        if result["code"] != 0:
            raise HTTPException(status_code=500, detail=f"获取数据库列表失败: {result['message']}")
        
        # 提取数据库名称
        # SHOW DATABASES查询不返回data字段，而是返回message字段
        if "data" in result:
            databases = [row[0] if isinstance(row, (list, tuple)) else row for row in result["data"]]
        else:
            # 如果没有data字段，说明查询不是SELECT类型，需要重新执行
            # 直接使用cursor获取结果
            try:
                cursor = db_utils.db.connection.cursor()
                cursor.execute("SHOW DATABASES")
                databases = [row[0] for row in cursor.fetchall()]
                cursor.close()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"获取数据库列表失败: {str(e)}")
        
        return {
            "status": "success",
            "data": databases,
            "count": len(databases),
            "timestamp": datetime.now().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据库列表失败: {str(e)}")

# 获取表列表
@app.get("/api/db/tables")
async def get_tables(database: str = Query(..., description="数据库名称")):
    """获取指定数据库中的所有表"""
    try:
        # 测试连接
        test_result = db_utils.db.test_connection()
        if test_result["code"] != 0:
            raise HTTPException(status_code=500, detail=f"无法连接到数据库: {test_result['message']}")
        
        # 选择数据库
        result = db_utils.db.use_database(database)
        if result["code"] != 0:
            raise HTTPException(status_code=400, detail=f"选择数据库失败: {result['message']}")
        
        # 执行查询获取表列表
        result = db_utils.db.execute_query("SHOW TABLES")
        if result["code"] != 0:
            raise HTTPException(status_code=500, detail=f"获取表列表失败: {result['message']}")
        
        # 提取表名称
        # SHOW TABLES查询不返回data字段，而是返回message字段
        if "data" in result:
            tables = [row[0] if isinstance(row, (list, tuple)) else row for row in result["data"]]
        else:
            # 如果没有data字段，说明查询不是SELECT类型，需要重新执行
            # 直接使用cursor获取结果
            try:
                cursor = db_utils.db.connection.cursor()
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]
                cursor.close()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"获取表列表失败: {str(e)}")
        
        return {
            "status": "success",
            "database": database,
            "data": tables,
            "count": len(tables),
            "timestamp": datetime.now().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取表列表失败: {str(e)}")
# 获取表结构
@app.get("/api/db/table/schema")
async def get_table_schema(
    database: str = Query(..., description="数据库名称"),
    table: str = Query(..., description="表名")
):
    """获取表的结构信息"""
    try:
        # 测试连接
        test_result = db_utils.db.test_connection()
        if test_result["code"] != 0:
            raise HTTPException(status_code=500, detail=f"无法连接到数据库: {test_result['message']}")
        
        # 选择数据库
        result = db_utils.db.use_database(database)
        if result["code"] != 0:
            raise HTTPException(status_code=400, detail=f"选择数据库失败: {result['message']}")
        
        # 获取表结构 - 使用DESCRIBE命令
        result = db_utils.db.execute_query(f"DESCRIBE `{table}`")
        if result["code"] != 0:
            raise HTTPException(status_code=500, detail=f"获取表结构失败: {result['message']}")
        
        # DESCRIBE查询不返回data字段，需要直接使用cursor
        try:
            cursor = db_utils.db.connection.cursor()
            cursor.execute(f"DESCRIBE `{table}`")
            columns_info = cursor.fetchall()
            cursor.close()
            
            # 格式化列信息
            columns = []
            for row in columns_info:
                columns.append({
                    "field": row[0],
                    "type": row[1],
                    "null": row[2],
                    "key": row[3],
                    "default": row[4],
                    "extra": row[5]
                })
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"获取表结构失败: {str(e)}")
        
        return {
            "status": "success",
            "database": database,
            "table": table,
            "data": columns,
            "count": len(columns),
            "timestamp": datetime.now().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取表结构失败: {str(e)}")

# 查询数据
@app.post("/api/db/query")
async def query_data(request: QueryRequest):
    """查询数据"""
    try:
        if not db_utils.db.connect():
            raise HTTPException(status_code=500, detail="无法连接到数据库")
        
        # 选择数据库
        result = db_utils.db.use_database(request.database)
        if result["code"] != 0:
            db_utils.db.close()
            raise HTTPException(status_code=400, detail=f"选择数据库失败: {result['message']}")
        
        # 构建查询
        columns = request.columns if request.columns else ["*"]
        where_clause = f" WHERE {request.where}" if request.where else ""
        limit_clause = f" LIMIT {request.limit}" if request.limit > 0 else ""
        offset_clause = f" OFFSET {request.offset}" if request.offset > 0 else ""
        
        query = f"SELECT {','.join(columns)} FROM {request.table}{where_clause}{limit_clause}{offset_clause}"
        
        # 执行查询
        result = db_utils.db.execute_query(query)
        
        if result["code"] == 0:
            return {
                "status": "success",
                "database": request.database,
                "table": request.table,
                "query": query,
                "data": result["data"],
                "count": len(result["data"]),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail=f"查询失败: {result['message']}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询数据失败: {str(e)}")
# 保存数据
@app.post("/api/db/save")
async def save_data(request: SaveRequest):
    """保存数据到数据库"""
    try:
        success = db_utils.save_to_database(
            data=request.data,
            table_name=request.table,
            database_name=request.database,
            additional_fields=request.additional_fields,
            check_duplicate=request.check_duplicate,
            duplicate_check_fields=request.duplicate_check_fields
        )
        
        if success:
            return {
                "status": "success",
                "database": request.database,
                "table": request.table,
                "count": len(request.data),
                "message": "数据保存成功",
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail="数据保存失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"保存数据失败: {str(e)}")

# QMS数据保存
@app.post("/api/qms/save")
async def save_qms_data(request: QMSSaveRequest):
    """保存QMS数据到数据库"""
    try:
        success = qms_db.save_qms_data(
            data=request.data,
            table_name=request.table,
            org_id=request.org_id,
            check_duplicate=request.check_duplicate,
            duplicate_check_fields=request.duplicate_check_fields
        )
        
        if success:
            return {
                "status": "success",
                "table": request.table,
                "org_id": request.org_id,
                "count": len(request.data),
                "message": "QMS数据保存成功",
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail="QMS数据保存失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"保存QMS数据失败: {str(e)}")

# 获取QMS数据
@app.post("/api/qms/fetch")
async def fetch_qms_data(
    table: str = Query(..., description="表名"),
    # org_id: str = Query(..., description="组织ID"),
    limit: int = Query(100, description="返回记录数限制"),
    offset: int = Query(0, description="偏移量")
):
    """从QMS表获取数据"""
    try:
        if not qms_db.db.connect():
            raise HTTPException(status_code=500, detail="无法连接到数据库")
        
        # 选择数据库
        result = qms_db.db.use_database(qms_db.default_database)
        if result["code"] != 0:
            raise HTTPException(status_code=400, detail=f"选择数据库失败: {result['message']}")
        
        # 构建查询
        # where_clause = f" WHERE org_id = '{org_id}'"
        where_clause = ""
        limit_clause = f" LIMIT {limit}" if limit > 0 else ""
        offset_clause = f" OFFSET {offset}" if offset > 0 else ""
        
        query = f"SELECT * FROM {table}{where_clause}{limit_clause}{offset_clause}"
        
        # 执行查询
        result = qms_db.db.execute_query(query)
        
        if result["code"] == 0:
            return {
                "status": "success",
                "table": table,
                # "org_id": org_id,
                "query": query,
                "data": result["data"],
                "count": len(result["data"]),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail=f"查询失败: {result['message']}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取QMS数据失败: {str(e)}")

# 获取Cookie
@app.post("/api/cookie/get")
async def get_cookie(request: CookieRequest):
    """获取指定平台的Cookie"""
    try:
        # 创建CookieTools实例
        cookie_tool = CookieTools(
            platform=request.platform,
            org_id=request.org_id,
            user=request.user if request.user else None
        )
        
        # 获取Cookie
        cookie = cookie_tool.getCookie()
        
        if cookie:
            return {
                "status": "success",
                "platform": request.platform,
                "org_id": request.org_id,
                "data": cookie,
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail="获取Cookie失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取Cookie失败: {str(e)}")
# 电机失效分析 - 录入
@app.post("/api/motor-failure-analysis/create")
async def create_motor_failure_analysis(request: MotorFailureAnalysisRequest):
    """录入电机失效分析数据"""
    try:
        # 准备数据
        data = [{
            "motor_model": request.motor_model,
            "motor_serial_no": request.motor_serial_no,
            "motor_detail_info": json.dumps(request.motor_detail_info) if request.motor_detail_info else None,
            "production_date": request.production_date,
            "failure_report_time": request.failure_report_time,
            "failure_occur_time": request.failure_occur_time,
            "failure_phenomenon": request.failure_phenomenon,
            "failure_type": request.failure_type,
            "failure_subtype": request.failure_subtype,
            "analysis_person": request.analysis_person,
            "analysis_department": request.analysis_department,
            "analysis_method": request.analysis_method,
            "failure_cause": request.failure_cause,
            "handling_measures": request.handling_measures,
            "analysis_status": request.analysis_status,
            "processing_result": request.processing_result,
            "remark": request.remark,
            "creator": request.creator,
            "ext1": request.ext1,
            "ext2": request.ext2,
            "ext3": request.ext3
        }]
        
        # 保存数据
        success = db_utils.save_to_database(
            data=data,
            table_name="motor_failure_analysis",
            database_name="qms",
            check_duplicate=True,
            duplicate_check_fields=["motor_serial_no"]
        )
        
        if success:
            return {
                "status": "success",
                "message": "电机失效分析数据录入成功",
                "data": {
                    "motor_serial_no": request.motor_serial_no,
                    "motor_model": request.motor_model
                },
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail="数据录入失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"录入电机失效分析数据失败: {str(e)}")

# 电机失效分析 - 查询
@app.post("/api/motor-failure-analysis/query")
async def query_motor_failure_analysis(request: MotorFailureAnalysisQueryRequest):
    """查询电机失效分析数据"""
    try:
        if not db_utils.db.connect():
            raise HTTPException(status_code=500, detail="无法连接到数据库")
        
        # 选择数据库
        result = db_utils.db.use_database("qms")
        if result["code"] != 0:
            db_utils.db.close()
            raise HTTPException(status_code=400, detail=f"选择数据库失败: {result['message']}")
        
        # 构建WHERE条件
        conditions = []
        if request.id:
            conditions.append(f"id = {request.id}")
        if request.motor_model:
            conditions.append(f"motor_model LIKE '%{request.motor_model}%'")
        if request.motor_serial_no:
            conditions.append(f"motor_serial_no LIKE '%{request.motor_serial_no}%'")
        if request.failure_type:
            conditions.append(f"failure_type = '{request.failure_type}'")
        if request.analysis_status:
            conditions.append(f"analysis_status = '{request.analysis_status}'")
        if request.analysis_person:
            conditions.append(f"analysis_person LIKE '%{request.analysis_person}%'")
        if request.start_date:
            conditions.append(f"failure_occur_time >= '{request.start_date} 00:00:00'")
        if request.end_date:
            conditions.append(f"failure_occur_time <= '{request.end_date} 23:59:59'")
        
        where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        limit_clause = f" LIMIT {request.limit}" if request.limit > 0 else ""
        offset_clause = f" OFFSET {request.offset}" if request.offset > 0 else ""
        
        # 构建查询
        query = f"SELECT * FROM motor_failure_analysis{where_clause} ORDER BY failure_report_time DESC{limit_clause}{offset_clause}"
        
        # 执行查询
        result = db_utils.db.execute_query(query)
        
        if result["code"] == 0:
            # 处理JSON字段
            for row in result["data"]:
                if row.get("motor_detail_info"):
                    try:
                        row["motor_detail_info"] = json.loads(row["motor_detail_info"])
                    except:
                        pass
            
            return {
                "status": "success",
                "query": query,
                "data": result["data"],
                "count": len(result["data"]),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail=f"查询失败: {result['message']}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询电机失效分析数据失败: {str(e)}")

# 电机失效分析 - 修改
@app.post("/api/motor-failure-analysis/update")
async def update_motor_failure_analysis(request: MotorFailureAnalysisUpdateRequest):
    """修改电机失效分析数据"""
    try:
        # 构建更新字段
        update_fields = []
        update_values = []
        
        if request.motor_model is not None:
            update_fields.append("motor_model = %s")
            update_values.append(request.motor_model)
        if request.motor_serial_no is not None:
            update_fields.append("motor_serial_no = %s")
            update_values.append(request.motor_serial_no)
        if request.motor_detail_info is not None:
            update_fields.append("motor_detail_info = %s")
            update_values.append(json.dumps(request.motor_detail_info))
        if request.production_date is not None:
            update_fields.append("production_date = %s")
            update_values.append(request.production_date)
        if request.failure_report_time is not None:
            update_fields.append("failure_report_time = %s")
            update_values.append(request.failure_report_time)
        if request.failure_occur_time is not None:
            update_fields.append("failure_occur_time = %s")
            update_values.append(request.failure_occur_time)
        if request.failure_phenomenon is not None:
            update_fields.append("failure_phenomenon = %s")
            update_values.append(request.failure_phenomenon)
        if request.failure_type is not None:
            update_fields.append("failure_type = %s")
            update_values.append(request.failure_type)
        if request.failure_subtype is not None:
            update_fields.append("failure_subtype = %s")
            update_values.append(request.failure_subtype)
        if request.analysis_person is not None:
            update_fields.append("analysis_person = %s")
            update_values.append(request.analysis_person)
        if request.analysis_department is not None:
            update_fields.append("analysis_department = %s")
            update_values.append(request.analysis_department)
        if request.analysis_method is not None:
            update_fields.append("analysis_method = %s")
            update_values.append(request.analysis_method)
        if request.failure_cause is not None:
            update_fields.append("failure_cause = %s")
            update_values.append(request.failure_cause)
        if request.handling_measures is not None:
            update_fields.append("handling_measures = %s")
            update_values.append(request.handling_measures)
        if request.analysis_status is not None:
            update_fields.append("analysis_status = %s")
            update_values.append(request.analysis_status)
        if request.processing_result is not None:
            update_fields.append("processing_result = %s")
            update_values.append(request.processing_result)
        if request.remark is not None:
            update_fields.append("remark = %s")
            update_values.append(request.remark)
        if request.ext1 is not None:
            update_fields.append("ext1 = %s")
            update_values.append(request.ext1)
        if request.ext2 is not None:
            update_fields.append("ext2 = %s")
            update_values.append(request.ext2)
        if request.ext3 is not None:
            update_fields.append("ext3 = %s")
            update_values.append(request.ext3)
        # 添加更新人和更新时间
        update_fields.append("updater = %s")
        update_values.append(request.updater)
        update_fields.append("update_time = %s")
        update_values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # 添加ID到WHERE条件
        update_values.append(request.id)
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="没有提供要更新的字段")
        
        # 连接数据库
        if not db_utils.db.connect():
            raise HTTPException(status_code=500, detail="无法连接到数据库")
        
        # 选择数据库
        result = db_utils.db.use_database("qms")
        if result["code"] != 0:
            db_utils.db.close()
            raise HTTPException(status_code=400, detail=f"选择数据库失败: {result['message']}")
        
        # 构建UPDATE语句
        query = f"UPDATE motor_failure_analysis SET {', '.join(update_fields)} WHERE id = %s"
        
        # 执行更新
        result = db_utils.db.execute_update(query, update_values)
        
        if result["code"] == 0:
            return {
                "status": "success",
                "message": "电机失效分析数据修改成功",
                "data": {
                    "id": request.id,
                    "updated_fields": len(update_fields) - 2
                },
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail=f"修改失败: {result['message']}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"修改电机失效分析数据失败: {str(e)}")

# 启动事件
@app.on_event("startup")
async def startup_event():
    """应用启动时执行"""
    print("数据库API服务启动中...")

# 关闭事件
@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时执行"""
    print("数据库API服务关闭中...")

if __name__ == "__main__":
    # 启动API服务
    uvicorn.run(
        "api_service:app",
        host="0.0.0.0",  # 监听所有网络接口
        port=8000,       # 端口号
        reload=True      # 开发模式下启用热重载
    )
