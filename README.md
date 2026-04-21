# Data Collection Dashboard

A Python-based data collection dashboard system that collects and analyzes user data across different regions and collection methods.

## Features

- Collects general statistics for registered users
- Region-wise data analysis
- Category-based classification
- Area tracking
- Item selection monitoring
- Mobile wallet usage statistics
- Engagement and charging metrics

## Project Structure

```
data_collection_dashboard/
├── config/
│   └── db_config.py          # Database configuration
├── modules/
│   ├── db.py                 # Database connection and query execution
│   ├── collector.py          # Main collection logic
│   ├── queries.py            # SQL query definitions
│   └── utils.py              # Utility functions (logging, etc.)
├── logs/                     # Log files directory
├── .env                      # Environment variables (credentials)
├── main.py                   # Entry point
└── requirements.txt          # Python dependencies

```

## Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure your database credentials in `.env` file:
   ```
   DB_HOST=your_host
   DB_PORT=3306
   DB_NAME=database
   DB_USER=username
   DB_PASSWORD=password
   ```
4. Run the application:
   ```bash
   python main.py
   ```

## Configuration

The system processes data for three collection methods:
- OBD (Outbound Dialing)
- IVR (Interactive Voice Response)
- SMS (Short Message Service)

And three regions:
- Region A
- Region B
- Region C

## Database Schema

The system expects the following main tables:
- `users` - Main user profiles
- `properties` - Property information
- `property_items` - Item selections
- `asset_properties` - Asset property data
- `asset_property_details` - Asset details
- `categories` - Category types
- `locations` - Geographic locations
- `collection_main` - General statistics (reporting DB)
- `collection_region` - Region-wise statistics (reporting DB)
- `collection_region_item` - Item-wise statistics (reporting DB)

## Logging

Logs are stored in the `logs/` directory:
- `main.log` - Main application logs
- `db.log` - Database operation logs
- `collector.log` - Collection process logs

## License

This project is not available for personal use.
