import time
import streamlit as st

class MockMetaflow:
    """Simulates a Metaflow run."""
    
    def __init__(self, run_id: int):
        self.run_id = run_id
        self.steps = ["start", "extract_data", "train_model", "evaluate", "end"]
        self.current_step_idx = 0
        
    def next_step(self):
        if self.current_step_idx < len(self.steps):
            step = self.steps[self.current_step_idx]
            self.current_step_idx += 1
            return step
        return None

    def visualize_flow(self):
        """Returns Graphviz source for the flow."""
        return """
        digraph Metaflow {
            rankdir=TB;
            node [shape=box, style="filled,rounded", fillcolor="white", width=2];
            
            start [label="start", fillcolor="#E3F2FD"];
            extract [label="extract_data", fillcolor="#FFF3E0"];
            train [label="train_model", fillcolor="#E8F5E9"];
            eval [label="evaluate", fillcolor="#F3E5F5"];
            end [label="end", fillcolor="#FFEBEE"];
            
            start -> extract;
            extract -> train;
            train -> eval;
            eval -> end;
        }
        """

class MockAirflow:
    """Simulates Airflow Scheduler."""
    
    @staticmethod
    def get_logs(dag_id: str, run_date: str):
        return f"""
        [2024-{run_date} 09:00:00] {{scheduler_job.py:1278}} INFO - Starting {dag_id}
        [2024-{run_date} 09:00:01] {{taskinstance.py:1150}} INFO - Marking task as RUNNING.
        [2024-{run_date} 09:00:05] {{local_task_job.py:102}} INFO - Task exited with return code 0
        [2024-{run_date} 09:00:06] {{dagrun.py:530}} INFO - Success: {dag_id}
        """
