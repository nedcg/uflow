import os
import re

files = [
    "./uflow_test.go", "./execute.go", "./uflow.go", 
    "./kafka/reprocessor.go", "./kafka/types.go", "./kafka/interceptors.go", 
    "./kafka/state_test.go", "./kafka/consumer.go", 
    "./kafka/saramax/reprocessor.go", "./kafka/saramax/adapter.go", 
    "./kafka/saramax/consumer.go", "./kafka/saramax/state.go", 
    "./kafka/state.go", "./kafka/main_test.go", "./uflow_benchmark_test.go",
    "./go.mod", "./kafka/go.mod", "./go.work", "./README.md"
]

for file in files:
    if not os.path.exists(file): continue
    with open(file, 'r') as f:
        content = f.read()
    
    content = content.replace("package flow\n", "package uflow\n")
    content = content.replace("package flow_test\n", "package uflow_test\n")
    content = content.replace("github.com/nedcg/flow", "github.com/nedcg/uflow")
    content = content.replace("flow.", "uflow.")
    
    with open(file, 'w') as f:
        f.write(content)
