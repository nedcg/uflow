import os
import re

files = [
    "./juzu_test.go", "./execute.go", "./juzu.go", 
    "./kafka/reprocessor.go", "./kafka/types.go", "./kafka/interceptors.go", 
    "./kafka/state_test.go", "./kafka/consumer.go", 
    "./kafka/saramax/reprocessor.go", "./kafka/saramax/adapter.go", 
    "./kafka/saramax/consumer.go", "./kafka/saramax/state.go", 
    "./kafka/state.go", "./kafka/main_test.go", "./juzu_benchmark_test.go",
    "./go.mod", "./kafka/go.mod", "./go.work", "./README.md"
]

words = [
    (r'\bInterceptor\b', "Step"),
    (r'\bNewExecution\b', "NewRunner"),
    (r'\bExecution\b', "Runner"),
    (r'\bEnterFunc\b', "InFunc"),
    (r'\bLeaveFunc\b', "OutFunc"),
    (r'\bErrorFunc\b', "CatchFunc"),
    (r'\bNewPipeline\b', "NewGroup"),
    (r'\bPipeline\b', "Group"),
    (r'\bNestedEnter\b', "NestedIn"),
    (r'\bNestedLeave\b', "NestedOut"),
    (r'(?<!\.)\bFunc\b', "StepFunc"),
    (r'(?<!\.)\bEnter\b', "In"),
    (r'(?<!\.)\bLeave\b', "Out"),
    (r'(?<!\.)\bError\b', "Catch"), 
    (r'\.Enter\b', ".In"),
    (r'\.Leave\b', ".Out"),
]

for file in files:
    if not os.path.exists(file): continue
    with open(file, 'r') as f:
        content = f.read()
    
    content = content.replace("package juzu\n", "package flow\n")
    content = content.replace("package juzu_test\n", "package flow_test\n")
    content = content.replace("github.com/nedcg/juzu", "github.com/nedcg/flow")
    content = content.replace("juzu.", "flow.")
    
    for old, new in words:
        content = re.sub(old, new, content)
        
    with open(file, 'w') as f:
        f.write(content)
