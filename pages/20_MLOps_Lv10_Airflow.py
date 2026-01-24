# -*- coding: utf-8 -*-
"""
MLOps Level 10: Airflow (Orchestration)
"""
import streamlit as st
import time
from src.mlops_utils import get_architect_note
from src.navigation import display_mlops_sidebar

def display_airflow_dag():
    st.header("1. The Orchestrator: Airflow")
    st.markdown("We combine everything into a **DAG (Directed Acyclic Graph)**.")
    
    st.graphviz_chart("""
    digraph Airflow {
        rankdir=LR;
        node [shape=box, style="rounded,filled", fillcolor=white, fontname="Arial"];
        edge [color="#666666"];
        
        Start [shape=circle, fillcolor="#4CAF50", label="Start", width=0.5];
        
        subgraph cluster_etl {
            label = "ETL";
            style = dashed;
            color = blue;
            Fetch [label="Fetch Data\n(Level 1)"];
            Preprocess [label="Preprocess\n(Level 2)"];
            Fetch -> Preprocess;
        }
        
        subgraph cluster_model {
            label = "Training";
            style = dashed;
            color = orange;
            AutoML [label="AutoML Train\n(Level 4)"];
            Validate [label="Validate RMSE\n(Level 19)"];
            AutoML -> Validate;
        }
        
        subgraph cluster_deploy {
            label = "Deployment";
            style = dashed;
            color = green;
            Push [label="Push to DVC\n(Level 8)"];
            Deploy [label="Deploy Docker\n(Level 19)"];
            Push -> Deploy;
        }
        
        End [shape=doublecircle, fillcolor="#F44336", label="End", width=0.5];
        
        Start -> Fetch;
        Preprocess -> AutoML;
        Validate -> Push [label=" If Passed "];
        Deploy -> End;
        
        # Failure path
        Validate -> End [label=" If Failed ", color=red, style=dotted];
    }
    """)

def run_orchestration():
    st.header("2. Run the Full Factory")
    
    if st.button("🏭 Execute DAG"):
        with st.status("Executing DAG...", expanded=True) as status:
            steps = [
                ("Fetch Data", 1),
                ("Preprocess", 1),
                ("AutoML Training", 2),
                ("Validation (RMSE < 5000)", 0.5),
                ("Version Data (DVC)", 1),
                ("Deploy Container", 1.5)
            ]
            
            for step, duration in steps:
                st.write(f"⚙️ Running: **{step}**...")
                time.sleep(duration)
                st.write(f"✅ {step} Complete")
                
            status.update(label="Pipeline Succeeded!", state="complete", expanded=False)
            
        st.balloons()
        st.success("The entire MLOps pipeline ran successfully without human intervention!")

def main():
    display_mlops_sidebar(20)
    
    st.title("🏭 Level 20: Airflow (Orchestration)")
    st.markdown("**'The Factory Manager'**")
    
    display_airflow_dag()
    st.markdown("---")
    run_orchestration()
    
    st.markdown("---")
    st.markdown(get_architect_note(20))

if __name__ == "__main__":
    main()
