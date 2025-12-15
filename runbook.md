# Operational Runbook: Healthcare Ops Platform

## 1. System Overview
**Componentes Críticos**:
- **Ingestão**: AWS Glue (Job: `ingest_bronze`)
- **Pipeline Principal**: AWS Step Functions (`orchestrator`)
- **ML Inference**: Scheduled Lambda/Glue every 15 min.
- **Monitoring**: CloudWatch Dashboard `HealthcareOps-Main`.

## 2. Common Incidents & Procedures

### [INC-01] ETL Pipeline Failure
**Sintoma**: Step Functions Execution Status `FAILED`. Alarm `PipelineFailure` disparado.
**Diagnóstico**:
1. Verificar Console Step Functions -> Graph Inspect. Identificar o step que falhou.
2. Se `RunBronzeIngest` falhou:
    - Checar logs do Glue Job no CloudWatch.
    - Erro comum: `Timeout` ou `S3 Access Denied`.
3. Se `RunDQ` falhou:
    - Dados violaram regras de qualidade.
    - Verificar relatório em `s3://.../gold/data_quality_runs/`.

**Ação (Remediação)**:
- Se erro transiente (network): **Retry** no Step Functions.
- Se erro de dados (DQ): Validar com time de negócio se a regra está  estrita demais ou se o dado está corrompido.

### [INC-02] Data Quality Failure
**Sintoma**: Alarme `DQRiskScoreHigh` ou Job `dq_check` status `FAILED`.
**Ação**:
1. Ler arquivo JSON de reporte no S3.
2. Se `null_count` > threshold: Contatar fornecedor do dado (Gerador).
3. Se `timestamp` fora do range: Checar fuso horário do gerador.

### [INC-03] Inference Degradation (Model Drift)
**Sintoma**: Reclamações de previsões imprecisas ou métrica de erro (MAPE) alta no Dashboard de ML.
**Diagnóstico**:
1. Verificar se o Job de Treino Semanal (`weekly_train`) rodou com sucesso no Domingo.
2. Comparar distribuição de features (`gold/features`) atual vs treino.
**Ação**:
- Disparar re-treino manual via Step Functions com `type="weekly_train"`.

## 3. Maintenance Tasks
- **Rotação de Chaves**: OpenAI API Key no Secrets Manager a cada 90 dias.
- **Logs Lifecycle**: Logs CloudWatch expirando em 30 dias (configurado no Terraform).
- **S3 Cleanup**: Objetos `raw/` movidos para Glacier após 30 dias.
