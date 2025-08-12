# Prometheus

## Pipelines
1. Profit
    - Unit-level profit needed for experiments
        - Primary Owner: Finance/Risk Team
        - Secondary Owner: Data Engineering Team 
        - On-call Schedule:
            - Monitored by BI in profit team
            - Data Engineers rotate watching pipeline on weekly basis
        - Common Issues:
            - Numbers don't align with numbers on accounts/fillings

    - Aggregate profit reported to investors
        - Primary Owner: Business Analytics Team
        - Secondary Owner: Data Engineering Team 
        - On-call Schedule:
            - Last week of the month, Data Engineers are monitoring that pipelines & their data are running smoothly
        - Common Issues:
            - A lot of missing values comming in from upstream tables messing with the metrics.

2. Growth
    - Aggregate growth reported to investors
        - Primary Owner: Business Analytics Team
        - Secondary Owner: Data Engineering Team 
        - On-call Schedule:
            - Last week of the month, Data Engineers are monitoring that pipelines & their data are running smoothly
        - Common Issues:
            - A lot of missing values comming in from upstream tables messing with the metrics.

    - Daily growth needed for experiments
        - Primary Owner: Business Analytics Team
        - Secondary Owner: Data Engineering Team 
        - On-call Schedule:
            - Monitored by BI in Business Analytics Team
            - Data Engineers rotate watching pipeline on weekly basis
        - Common Issues:
            - Numbers don't align with total active users
            

3. Engagement
    - Aggregate engagement reported to investors
        - Primary Owner: Business Analytics Team
        - Secondary Owner: Data Engineering Team 
        - On-call Schedule:
            - Last week of the month, Data Engineers are monitoring that pipelines & their data are running smoothly
        - Common Issues:
            - A lot of missing values comming in from upstream tables messing with the metrics.
