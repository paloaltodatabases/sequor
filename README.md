# Sequor
Instead of using rigid SaaS connectors with growing costs, build complete API workflows that connect your database to any API using familiar YAML and SQL. Your requirements fully met with zero per-row costs.

Sequor fuses API execution with your database, enabling bidirectional data flow between APIs and database tables. By storing intermediate data in your database, you can leverage the full power of SQL for transformations, analytics, and business logic. This unified execution model eliminates the traditional boundary between iPaaS-style app integration and ETL-style data pipelines.

With Sequor's code-first approach (YAML for flows, Jinja or Python for dynamic parameters, and SQL for logic), you can apply software engineering best practices to integrations: version control, collaboration, CI/CD, and local development.

# Core capabilities
* **YAML for clear workflow structure**: Define integration flows in readable YAML. Version control friendly, no vendor lock-in. Built-in reliability: validation, retries, task-level observability.

* **Dynamic expressions for flexible logic**: Use Jinja templates `{{ var() }}` for environment variables. Add `_expression` suffix to compute with Python. Infinite customization within YAML structure.

* **Built-in database integration**: Iterate over input tables to make API calls per record. Every HTTP response maps to database tables. Seamless data flow without custom glue code.

# Example: Pull Shopify orders → Compute customer metrics in Snowflake → Update Mailchimp
Create a Sequor project with the following 3-step flow. You get an end-to-end solution in minutes with just two operations: **transform** for running SQL and **http_request** for API interactions. 

## Step 1: Fetch orders from Shopify API
Pull orders data into Snowflake. Jinja for source-level variables. Python for one-line response navigation.
```yaml
- op: http_request
  request:
    source: "shopify"
    url: "https://{{ var('store_name') }}.myshopify.com/admin/api/{{ var('api_version') }}/orders.json"
    method: GET
    parameters:
      status: any
    headers:
      "Accept": "application/json"
  response:
    success_status: [200]
    tables:
      - source: "snowflake"
        table: "shopify_orders"
        columns: {
          "id": "text", "customer_id": text, "email": "text", 
          "created_at": "text", "total_price": "text", "total_items": "text"
        }
        data_expression: response.json()['orders']
```

## Step 2: Compute customer metrics in SQL
Calculate total spend and order count per customer in Snowflake.
```yaml
- op: transform
  source: "snowflake"
  target_table: "customer_metrics"
  query: |
    SELECT
      email,
      SUM(total_price::decimal) as total_spent,
      COUNT(*) as order_count
    FROM shopify_orders
    WHERE email IS NOT NULL
    GROUP BY email
```

## Step 3: Update Mailchimp API
Push customer metrics to Mailchimp. Python is used for advanced URL construction and flexible body formation.
```yaml
- op: http_request
  for_each:
    source: "snowflake"
    table: "customer_metrics"
    as: customer
  request:
    source: "mailchimp"
    url_expression: |
      email = var('customer')['email']
      import hashlib
      subscriber_hash = hashlib.md5(email.lower().encode()).hexdigest()
      return "https://{{ var('dc') }}.api.mailchimp.com/{{ var('api_version') }}/lists/{{ var('mailchimp_list_id') }}/members/" + subscriber_hash
    method: PATCH
    body_format: json
    body_expression: |
        customer = var('customer')
        return {
          "merge_fields": {
            "TOTALSPENT": float(customer['total_spent']),
            "ORDERCOUNT": customer['order_count']
          }
        }
  response:
    success_status: [200]
```


# Getting started
* [Install Sequor](https://docs.sequor.dev/getting-started/installation). Easy to get started with `pip install sequor`.
* [Follow Quickstart](https://docs.sequor.dev/getting-started/quickstart)
* [Explore examples of real-life integrations](https://github.com/paloaltodatabases/sequor-integrations)
* [Documentation](https://docs.sequor.dev/)

# Community
* [Discuss Sequor on GitHub](https://github.com/paloaltodatabases/sequor/discussions) - To get help and participate in discussions about best practices, or any other conversation that would benefit from being searchable

# Stay connected
* [Subscribe to our newsletter](https://buttondown.com/sequor) -  Updates on new releases and features, guides, and case studies.






  
