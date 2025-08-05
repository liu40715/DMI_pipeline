from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List  # 這行是你缺少的
import schemas, models, database

# 建立資料表（第一次執行才需要）
models.Base.metadata.create_all(bind=database.engine)

app = FastAPI()

# 取得 DB Session 依賴
def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

# [C] Create 新增 DAG
@app.post("/dags/", response_model=schemas.DAG)
def create_dag(dag: schemas.DAG, db: Session = Depends(get_db)):
    existing = db.query(models.DAGModel).filter(models.DAGModel.dag_id == dag.dag_id).first()
    if existing:
        raise HTTPException(status_code=400, detail="DAG with this dag_id already exists")
    db_dag = models.DAGModel(
        dag_id=dag.dag_id,
        schedule_interval=dag.schedule_interval,
        start_date=dag.start_date,
        catchup=dag.catchup,
        tasks=dag.tasks,
    )
    db.add(db_dag)
    db.commit()
    db.refresh(db_dag)
    return db_dag


# [R-1] 讀取所有 DAGs
@app.get("/dags/", response_model=List[schemas.DAG])
def read_dags(db: Session = Depends(get_db)):
    return db.query(models.DAGModel).all()


# [R-2] 讀取指定 DAG
@app.get("/dags/{dag_id}", response_model=schemas.DAG)
def read_dag(dag_id: str, db: Session = Depends(get_db)):
    dag = db.query(models.DAGModel).filter(models.DAGModel.dag_id == dag_id).first()
    if not dag:
        raise HTTPException(status_code=404, detail="DAG not found")
    return dag


# [U] 更新整個 DAG
@app.put("/dags/{dag_id}", response_model=schemas.DAG)
def update_dag(dag_id: str, dag: schemas.DAG, db: Session = Depends(get_db)):
    db_dag = db.query(models.DAGModel).filter(models.DAGModel.dag_id == dag_id).first()
    if not db_dag:
        raise HTTPException(status_code=404, detail="DAG not found")

    db_dag.schedule_interval = dag.schedule_interval
    db_dag.start_date = dag.start_date
    db_dag.catchup = dag.catchup
    db_dag.tasks = dag.tasks

    db.commit()
    db.refresh(db_dag)
    return db_dag


# [D] 刪除 DAG
@app.delete("/dags/{dag_id}")
def delete_dag(dag_id: str, db: Session = Depends(get_db)):
    db_dag = db.query(models.DAGModel).filter(models.DAGModel.dag_id == dag_id).first()
    if not db_dag:
        raise HTTPException(status_code=404, detail="DAG not found")
    db.delete(db_dag)
    db.commit()
    return {"message": "DAG deleted", "dag_id": dag_id}
