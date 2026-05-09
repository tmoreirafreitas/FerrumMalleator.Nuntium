# ⚒️ Nuntium Sample — OutboxFlow

Sample oficial demonstrando Outbox Pattern com o Nuntium.

Este sample apresenta:

- Kafka
- Outbox Pattern
- Persistência transacional
- Eventual consistency
- OpenTelemetry-compatible instrumentation
- Observabilidade distribuída
- Persistência com EF Core InMemory

---

# 🚀 Objetivo

Demonstrar como o Nuntium implementa mensageria confiável utilizando Outbox Pattern.

O fluxo implementado simula:

```text
Application
    ↓
Outbox Store
    ↓
Outbox Processor
    ↓
Kafka Publisher
    ↓
Kafka Consumer
```

---

# 🧠 O que este sample demonstra

- Outbox Pattern
- Persistência confiável de mensagens
- Eventual consistency
- Publicação desacoplada
- Processamento assíncrono
- Observabilidade distribuída
- Kafka transport
- Instrumentação semântica

---

# 📦 Tecnologias utilizadas

- .NET 10
- Apache Kafka
- Entity Framework Core InMemory
- OpenTelemetry
- Serilog
- FerrumMalleator.Nuntium
- FerrumMalleator.Nuntium.Persistence.EntityFramework

---

# 🐳 Subindo infraestrutura

Na pasta `samples/` execute:

```bash
docker compose up -d
```

A infraestrutura iniciará:

- Kafka
- Kafka UI

---

# 🔌 Endpoints

Kafka:

```text
localhost:29092
```

Kafka UI:

```text
http://localhost:8082
```

---

# ▶️ Executando o sample

Na pasta do projeto:

```bash
dotnet run
```

---

# 📈 O que você verá

O worker publicará continuamente mensagens:

```text
PedidoCriado
```

Porém a publicação não ocorre diretamente no Kafka.

O fluxo real será:

```text
Application
    ↓
Outbox
    ↓
Outbox Processor
    ↓
Kafka
    ↓
Consumer
```

O console exibirá:

- Outbox processing
- Kafka publish pipeline
- Distributed tracing
- Metrics
- Semantic activities
- Consumer processing

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
Worker
   ↓
OutboxPublisher
   ↓
OutboxStore
   ↓
OutboxProcessor
   ↓
KafkaPublisher
   ↓
KafkaTransport
   ↓
Apache Kafka
   ↓
Consumer
```

---

# 🛠️ Persistência

O sample utiliza:

```text
Entity Framework Core InMemory
```

mantendo o ambiente simples para execução local.

---

# 📊 Métricas disponíveis

O sample demonstra métricas como:

- `messages.published`
- `messages.consumed`
- `publish.duration.ms`
- `outbox.process.duration.ms`
- `kafka.publish.duration.ms`
- `consumer.duration.ms`

---

# ⚠️ Registro de mensagens

Cada tipo de mensagem deve ser registrado apenas uma vez utilizando `WithTopic<T>()`.

Exemplo correto:

```csharp
bus.WithTopic<PedidoCriado>(
    "pedido-criado",
    "outbox-flow");

bus.AddConsumer<PedidoConsumer, PedidoCriado>();
```

Evite registrar o mesmo tipo múltiplas vezes.

---

# 🛠️ Encerrando infraestrutura

Na pasta `samples/` execute:

```bash
docker compose down
```

---

# ❤️ Nuntium

Mensageria moderna, observável e poderosa — sem complexidade desnecessária.