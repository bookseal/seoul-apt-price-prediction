# -*- coding: utf-8 -*-
"""
MLOps Level 6: CI/CD (GitHub Actions)
"""
import streamlit as st
import time
from src.mlops_utils import get_architect_note
from src.navigation import display_mlops_sidebar

def display_workflow_viz():
    st.header("1. The CI/CD Pipeline")
    st.markdown("We don't deploy from our laptop. **GitHub** does it for us.")
    
    st.graphviz_chart("""
    digraph CICD {
        rankdir=LR;
        node [shape=box, style=filled, fillcolor=white];
        
        User [shape=oval, fillcolor="#E3F2FD", label="Data Scientist"];
        GitHub [fillcolor="#24292e", fontcolor=white, label="GitHub Repo"];
        
        subgraph cluster_actions {
            label = "GitHub Actions (Runner)";
            style = dashed;
            color = grey;
            
            Test [label="pytest", color=red];
            Build [label="docker build", color=blue];
            Deploy [label="ssh deploy", color=green];
            
            Test -> Build -> Deploy;
        }
        
        Server [shape=component, fillcolor="#FFF3E0", label="Production Server"];
        
        User -> GitHub [label="git push"];
        GitHub -> Test [label="Trigger"];
        Deploy -> Server [label="Update"];
    }
    """)

def simulate_cicd():
    st.header("2. Simulation: git push origin main")
    
    if st.button("🚀 Push Code"):
        with st.status("Running GitHub Actions...", expanded=True) as status:
            st.write("🔹 Job: Build & Test")
            time.sleep(1)
            st.write("✅ Checkout code")
            time.sleep(0.5)
            st.write("✅ Install dependencies (pip install -r requirements.txt)")
            time.sleep(0.5)
            st.write("✅ Run tests (pytest src/)")
            time.sleep(1)
            
            st.write("🔹 Job: Deploy")
            st.write("✅ Log in to Docker Hub")
            time.sleep(0.5)
            st.write("✅ Build and Push Image")
            time.sleep(1)
            st.write("✅ SSH to Production Server -> docker pull -> restart")
            
            status.update(label="Deployment Complete!", state="complete", expanded=False)
            
        st.balloons()
        st.success("Your new model is live!")

def main():
    display_mlops_sidebar(16)
    
    st.title("♾️ Level 16: CI/CD")
    st.markdown("**'Continuous Integration, Continuous Deployment'**")
    
    display_workflow_viz()
    st.markdown("---")
    simulate_cicd()
    
    st.markdown("---")
    st.markdown(get_architect_note(16))

if __name__ == "__main__":
    main()
