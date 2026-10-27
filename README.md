# pipeline-iot-LoRa2026
Pipeline de dados para o Agrimesh, projeto de rede mesh na área agrícola com uso de LoRa e Meshtastic

Agradecimentos e referências à infraestrutura base e orientações do projeto podem ser consultados no perfil github.com/siriani.

## Como executar

### Pré-requisitos
1. Docker e Docker-compose instalados na máquina

### Instruções de Execução
1. Crie um arquivo .env preenchido conforme especificado em .env.example
2. Execute:

```bash
docker-compose up -d --build
```

### Acompanhar Logs em tempo real
1. Após a rodar o programa, execute:

```bash
docker-compose logs -f pipeline
```