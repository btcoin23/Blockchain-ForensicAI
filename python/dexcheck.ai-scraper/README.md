# Crypto Trader Data Scraper

A Python application that scrapes top crypto trader data from dexcheck.ai and stores it in a MySQL database using Prisma ORM.

## Features

- Automated web scraping using Selenium
- Data parsing with BeautifulSoup4
- Database storage with Prisma ORM and MySQL
- Asynchronous data handling

## Prerequisites

- Python 3.8+
- MySQL Server
- Chrome Browser

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
```

2. Install Python dependencies:
```bash
pip install selenium webdriver-manager beautifulsoup4 prisma
```

3. Set up the database:

- Create a MySQL database named trader_db
- Copy .env.example to .env and update database credentials:
```bash
DATABASE_URL="mysql://user:password@localhost:3306/trader_db"
```

4. Initialize Prisma:
```bash
prisma generate
prisma db push
```

## Usage

Run the scraper:
```bash
python main.py
```

## Project Structure
```bash
├── main.py              # Main scraper script
├── prisma/
│   └── schema.prisma    # Database schema
├── .env                 # Environment variables
└── README.md           # Documentation
```
## Database Schema

model Trader {
  id        Int      @id @default(autoincrement())
  address   String   @unique
  pnl       String?
  trades    Int?
  winRate   String?
  avgRoi    String?
  createdAt DateTime @default(now())
  updatedAt DateTime @updatedAt
}

## Data Fields

- address: Trader's wallet address
- pnl: Profit and Loss
- trades: Number of trades
- winRate: Win rate percentage
- avgRoi: Average Return on Investment
