# ⚒️ Nuntium Sample — SagaFlow

Sample oficial demonstrando Saga Orchestration com o Nuntium.

Este sample apresenta:

- Kafka
- Saga orchestration
- Stateful workflow
- OpenTelemetry-compatible instrumentation
- Observabilidade distribuída
- Persistência de estado com EF Core InMemory

---

# 🚀 Objetivo

Demonstrar como o Nuntium coordena workflows distribuídos utilizando Sagas.

O fluxo implementado simula um processo de pedido:

```text
PedidoCriado
      ↓
[SAGA]
      ↓
AprovarPagamento
      ↓
[CONSUMER]
      ↓
PagamentoAprovado
      ↓
[SAGA]
      ↓
SepararEstoque
      ↓
[CONSUMER]
      ↓
EstoqueFinalizado
      ↓
[SAGA]
      ↓
Pedido Finalizado
```

---

# 🧠 O que este sample demonstra

- Saga orchestration
- Workflow distribuído
- Coordenação baseada em eventos
- Separação entre Commands e Events
- Persistência de estado da saga
- Observabilidade distribuída
- Instrumentação semântica
- Kafka transport

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

O sample utiliza Docker Compose para subir:

- Kafka
- Kafka UI

Execute:

```bash
docker compose up -d
```

Kafka ficará disponível em:

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

O worker publicará continuamente:

```text
PedidoCriado
```

A saga coordenará automaticamente o workflow:

```text
PedidoCriado
→ AprovarPagamento
→ PagamentoAprovado
→ SepararEstoque
→ EstoqueFinalizado
→ Pedido Finalizado
```

Além disso, o console exibirá:

- Activities
- Traces
- Metrics
- Kafka instrumentation
- Saga instrumentation
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
Worker
   ↓
KafkaPublisher
   ↓
KafkaTransport
   ↓
Apache Kafka
   ↓
Saga
   ↓
Consumers
   ↓
Eventos subsequentes
```

---

# 🛠️ Persistência da Saga

O estado da saga é persistido utilizando:

```text
Entity Framework Core InMemory
```

mantendo o sample simples e fácil de executar localmente.

---

# ⚠️ Registro de mensagens

Cada tipo de mensagem deve ser registrado apenas uma vez utilizando `WithTopic<T>()`.

O Nuntium utiliza um registro único de metadata por mensagem para:

- resolução de tópicos
- serialização
- roteamento
- observabilidade
- particionamento

Exemplo correto:

```csharp
bus.AddSaga()
   .WithTopic<PedidoCriado>(
       "pedido-criado",
       "saga-flow");

bus.AddConsumer<PedidoConsumer, PedidoCriado>();
```

Evite registrar a mesma mensagem múltiplas vezes:

```csharp
// ❌ incorreto
bus.WithTopic<PedidoCriado>(
    "pedido-criado",
    "group");

bus.AddConsumer<PedidoConsumer, PedidoCriado>()
   .WithTopic<PedidoCriado>(
       "pedido-criado",
       "group");
```

---

# 🛠️ Encerrando infraestrutura

```bash
docker compose down
```

---

# ❤️ Nuntium

Mensageria moderna, observável e poderosa — sem complexidade desnecessária.