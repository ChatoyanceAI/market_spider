from airflow.api.common.experimental import pool


def create_pool(name: str, slots: int, description: str):
    return pool.create_pool(name, slots, description)


def get_task_xcom(task_id, context):
    task_instance = context['task_instance']
    return task_instance.xcom_pull(task_ids=task_id)
