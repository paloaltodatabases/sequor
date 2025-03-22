# run_flow.py
from project.project import Project

# Initialize a project
project = Project("/Users/maximgrinev/myprogs/sequor_projects/misc")

# Get a specific flow
customer_flow = project.get_flow("first_flow")

# Execute the flow
result = customer_flow.run(context={})

# Print some results
print(f"Flow execution completed with result: {result}")