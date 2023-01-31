from fastapi import FastAPI
from multiprocessing import Process

import pipeline

app = FastAPI()


@app.get("/{task}")
def gateway(task: str, seed: int = 0):
    if task == "generate_df":
        addr = pipeline.get_local_addr()
        p = Process(target=pipeline.generate_df_no_shared, args=(addr,))
        p.start()
        p.join()
    elif task == "consume_df":
        addr = pipeline.get_local_addr()
        p = Process(target=pipeline.consume_df_no_shared, args=(addr, seed))
        p.start()
        p.join()
    else:
        raise ValueError(f"Invalid task: {task}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("airflow_server:app", host="0.0.0.0", port=18080, workers=16)
