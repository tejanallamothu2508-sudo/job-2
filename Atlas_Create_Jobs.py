# Databricks notebook source
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, TaskDependency, JobSettings, RunIf, Source,
)

w = WorkspaceClient()

NB_IGTEXCEL_BRONZE = "/Workspace/Shared/Excel_Poc/Job_IGTEXCEL_BRONZE"
NB_PIPELINE_HISTORY_ADD = "/Workspace/Shared/Pdf_Poc/Task_PIPELINE_HISTORY_ADD"
NB_PIPELINE_HISTORY_UPDATE = "/Workspace/Shared/Pdf_Poc/Task_PIPELINE_HISTORY_UPDATE"
CLUSTER_ID = "0127-183944-kgzttd00"

pipeline_names = [
    "Atlas_Bronze_CDS_CII_Billing_Manual",
    "Atlas_Bronze_ZRBINQ_Billing_Plans",
    "Atlas_Bronze_Alliant_Royalties_Alliant_Queries_Finance_12_2025",
    "Atlas_Bronze_Alliant_Royalties_Alliant_Queries_Finance_GL_113015_12_2025",
    "Atlas_Bronze_Depreciation_Schedule",
    "Atlas_Bronze_Daily_Fee_Poker_Install_Base",
    "Atlas_Bronze_Machine_Sales_Revenue_Plugs",
    "Atlas_Bronze_Machine_Sales_Cost_Plugs",
    "Atlas_Bronze_Flat_Fee_Billing",
    "Atlas_Bronze_Lottery_Billing",
    "Atlas_Bronze_Africa_Billing",
    "Atlas_Bronze_EMEA_Fixed_Fee_Billing",
    "Atlas_Bronze_Greece_WLA_Billing",
    "Atlas_Bronze_Iceland_Billing",
    "Atlas_Bronze_LAC_Billing",
    "Atlas_Bronze_Participation_Billing_Regular",
    "Atlas_Bronze_Participation_Billing_Accruals",
    "Atlas_Bronze_CDS_CII_Billing_Regular",
    "Atlas_Bronze_WAP_Canada_Discounts",
    "Atlas_Bronze_WAP_Billing",
    "Atlas_Bronze_ZRBINQ_Billing_Plans_CoCode_1000",
    "Atlas_Bronze_ZRBINQ_Billing_Plans_CoCode_1460",
    "Atlas_Bronze_ZRBINQ_Billing_Plans_GSMA",
    "Atlas_Bronze_ZRBINQ_Billing_Plans_Hybrid_1000",
    "Atlas_Bronze_ZRBINQ_Billing_Plans_Hybrid_1460",
]

def build_tasks():
    return [
        Task(
            task_key="Task_PIPELINE_HISTORY_ADD",
            notebook_task=NotebookTask(
                notebook_path=NB_PIPELINE_HISTORY_ADD,
                source=Source.WORKSPACE,
            ),
            existing_cluster_id=CLUSTER_ID,
        ),
        Task(
            task_key="Job_IGTEXCEL_BRONZE",
            depends_on=[TaskDependency(task_key="Task_PIPELINE_HISTORY_ADD")],
            notebook_task=NotebookTask(
                notebook_path=NB_IGTEXCEL_BRONZE,
                source=Source.WORKSPACE,
            ),
            existing_cluster_id=CLUSTER_ID,
        ),
        Task(
            task_key="Task_PIPELINE_HISTORY_UPDATE",
            depends_on=[TaskDependency(task_key="Job_IGTEXCEL_BRONZE")],
            run_if=RunIf.ALL_SUCCESS,
            notebook_task=NotebookTask(
                notebook_path=NB_PIPELINE_HISTORY_UPDATE,
                source=Source.WORKSPACE,
                base_parameters={
                    "job_run_id": "{{job.run_id}}",
                    "pipeline_status": "1",
                },
            ),
            existing_cluster_id=CLUSTER_ID,
        ),
        Task(
            task_key="Task_PIPELINE_HISTORY_UPDATE_FAILURE",
            depends_on=[TaskDependency(task_key="Job_IGTEXCEL_BRONZE")],
            run_if=RunIf.AT_LEAST_ONE_FAILED,
            notebook_task=NotebookTask(
                notebook_path=NB_PIPELINE_HISTORY_UPDATE,
                source=Source.WORKSPACE,
                base_parameters={
                    "job_run_id": "{{job.run_id}}",
                    "pipeline_status": "3",
                },
            ),
            existing_cluster_id=CLUSTER_ID,
        ),
    ]

results = []
for name in pipeline_names:
    existing = list(w.jobs.list(name=name))
    tasks = build_tasks()
    if existing:
        job_id = existing[0].job_id
        print(f'  {name} — already exists (job_id={job_id}), updating...')
        w.jobs.reset(job_id=job_id, new_settings=JobSettings(
            name=name, max_concurrent_runs=1, tasks=tasks,
        ))
        results.append((name, job_id, 'updated'))
    else:
        resp = w.jobs.create(name=name, max_concurrent_runs=1, tasks=tasks)
        job_id = resp.job_id
        print(f'  {name} — created (job_id={job_id})')
        results.append((name, job_id, 'created'))

print(f'\nDone: {len(results)} jobs')
for name, jid, action in results:
    print(f'  {jid}: {name} ({action})')
