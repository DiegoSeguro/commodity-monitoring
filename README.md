# PADAP Data Pipeline

A data pipeline for collecting, processing, and analyzing commodity price data across Brazil at municipal granularity, powered by the CONAB public API.

---

## Overview

This project provides an automated pipeline for ingesting and transforming agricultural commodity pricing data from **CONAB** (**Companhia Nacional de Abastecimento** — Brazil's National Supply Company), enabling price analysis at the municipal level across the entire country.

The data covers weekly pricing reports for agricultural commodities, structured by municipality, allowing analysts and researchers to track price variations, identify regional patterns, and support decision-making in the agricultural sector.

---

## Demonstration: PADAP — Alto Paranaíba Settlement Program

The initial implementation of this pipeline was developed with a focus on the **PADAP (Programa de Assentamento Dirigido do Alto Paranaíba)**, a large-scale agricultural settlement program located in the state of **Minas Gerais**, within the Brazilian Cerrado biome.

PADAP encompasses some of the most productive agricultural land in the Brazilian Cerrado, with its core operations concentrated in the municipalities of:

- **São Gotardo**, MG
- **Santa Juliana**, MG

These municipalities are among the leading producers of vegetables, grains, and other commodities in the Cerrado region, making municipal-level price tracking particularly valuable for producers, cooperatives, and policymakers operating in the area.

---

## National Coverage

Although the demonstration is centered on the Alto Paranaíba region, the pipeline is designed for **nationwide deployment**. Since CONAB provides standardized pricing data for municipalities across all Brazilian states, this solution can be extended to any region of the country without changes to the core architecture.

Potential use cases include:

- Regional commodity price monitoring across Brazilian states
- Historical price trend analysis at the municipal level
- Integration with agricultural planning and supply chain systems
- Support for public policy and market intelligence research

---

## Data Source

All commodity pricing data is sourced from the **CONAB Open Data API**, which provides weekly municipal pricing reports in a standardized format. For more information, visit [dados.conab.gov.br](https://dados.conab.gov.br).

---

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/padap-pipeline.git
   cd padap-pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the project root:
   ```env
   API_URL=https://your-api.com/data
   API_TOKEN=your_secret_token
   CONAB_URL=https://dados.conab.gov.br/file.txt
   ```

4. Run the pipeline:
   ```bash
   python padap_pipeline.py
   ```

---

## Output

After execution, the following files are written to the `output/` directory:

| File | Description |
|---|---|
| `data_tratada.csv` | Full processed and merged dataset |
| `datas_tratadas.csv` | Deduplicated list of dates present in the data |
| `municipios.csv` | Deduplicated list of municipalities present in the data |

---

## Project Structure

```
padap-pipeline/
├── padap_pipeline.py     # Main pipeline script
├── requirements.txt      # Python dependencies
├── .env                  # Environment variables (do not commit)
├── .gitignore
└── README.md
```

---

## Requirements

- Python 3.10+
- `requests`
- `pandas`
- `python-dotenv`

---

## License

This project is intended for research and analytical use. Please refer to CONAB's terms of service regarding the use of their public data.
