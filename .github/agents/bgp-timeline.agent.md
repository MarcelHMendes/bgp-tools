# Agente de Processamento de Experimentos BGP

## Descrição

Este agente otimizado processa experimentos BGP a partir de arquivos JSON, extraindo informações temporais específicas para muxes selecionados e gerando arquivos de configuração estruturados.

## Funcionalidade Principal

O agente recebe como entrada:
- Uma lista de experimentos no formato JSON
- Um `goodsite` (mux que se repete nos experimentos, ex: "vtrchicago")
- Um `mux_adicional` (outro mux a ser analisado, ex: "grnet01")

## Processamento

### 1. Extração de Dados

Para cada experimento, o agente:
- Identifica todos os `rounds` disponíveis
- Filtra apenas rodadas que contêm `announce` (ignora rodadas apenas com `withdraw`)
- Para cada prefixo dentro de cada rodada, identifica os muxes listados em `announce`

### 2. Cálculo dos Períodos

Para cada mux dentro de cada rodada:
- **Início do período**: `round_start_times[round_index]`
- **Fim do período**: `round_start_times[round_index + 1]` (ou horário final do experimento para última rodada)

### 3. Mapeamento de Prefixos por Tipo

Para cada experimento, o agente classifica prefixos em:
- **Valid**: Prefixo anunciado APENAS pelo mux com ORIGIN válido (61574)
- **Invalid**: Prefixo anunciado APENAS pelo mux com ORIGIN inválido (47065)
- **Non-invalid**: Prefixo não classificado como inválido

## Estrutura de Saída

```json
{
  "seventh_experiment_goodsite": {
    "bgpdump": "",
    "traceroute_file": "",
    "prefixes": {
      "non_invalid": "138.185.228.0/24",
      "invalid": "138.185.229.0/24",
      "valid": "138.185.231.0/24"
    },
    "location": {
      "goodsite": {
        "start": "2026-06-03T07:30:00.304506Z",
        "end": "2026-06-03T12:00:00.275921Z"
      },
      "mux_adicional": {
        "start": "2026-06-03T07:30:00.304506Z",
        "end": "2026-06-03T12:00:00.275921Z"
      }
    }
  }
}
