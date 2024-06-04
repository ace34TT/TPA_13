from airflow.operators.bash_operator import BashOperator


def create_bash_task(task_id, bash_command, task_group=None):
    if task_group:
        return BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            dag=dag,
            task_group=task_group,
        )
    else:
        return BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            dag=dag,
        )
