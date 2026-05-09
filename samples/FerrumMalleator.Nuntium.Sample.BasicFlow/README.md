# ⚒️ Nuntium Sample — BasicFlow

Sample oficial demonstrando o uso básico do Nuntium com:

- Kafka
- Producer
- Consumer
- Observabilidade
- OpenTelemetry-compatible instrumentation

---

# 🚀 Objetivo

Este sample demonstra um fluxo simples de mensageria:

```text
Producer → Kafka → Consumer
```

incluindo:

- publicação de mensagens
- consumo de mensagens
- tracing distribuído
- métricas
- instrumentação semântica

---

# 📦 Tecnologias utilizadas

- .NET 8
- Apache Kafka
- OpenTelemetry
- Serilog
- FerrumMalleator.Nuntium

---

# 🐳 Subindo infraestrutura

O sample utiliza Docker Compose para subir:

- Kafka
- Zookeeper
- Kafka UI

Execute:

```bash
docker compose up -d
```

Kafka ficará disponível em:

```text
localhost:9092
```

Kafka UI:

```text
http://localhost:8080
```

---

# ▶️ Executando o sample

Na pasta do projeto:

```bash
dotnet run
```

---

# 📈 O que você verá

O sample publicará mensagens continuamente:

```text
Pedido publicado: xxxxx
```

E o consumer processará automaticamente os eventos.

Além disso, o console exibirá:

- Activities
- Traces
- Metrics
- Semantic messaging tags

---

# 🔭 Observabilidade

O Nuntium utiliza instrumentação nativa do .NET através de:

- `ActivitySource`
- `Activity`
- `Meter`

permitindo integração transparente com:

- OpenTelemetry
- Jaeger
- Grafana
- Prometheus
- OTLP exporters
- Seq

---

# 🧩 Estrutura simplificada

```text
IMessagePublisher
        ↓
KafkaPublisher
        ↓
KafkaTransport
        ↓
Apache Kafka
```

---

# 🛠️ Encerrando infraestrutura

```bash
docker compose down
```

---

# ❤️ Nuntium

Mensageria moderna, observável e poderosa — sem complexidade desnecessária.