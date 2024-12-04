import os
import json
from datetime import datetime, timedelta
import anthropic
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict
import random

class FinancialDataGenerator:
    def __init__(self):
        self.client = anthropic.Anthropic(
            api_key=os.getenv("ANTHROPIC_API_KEY")
        )
        self.db_params = {
            "dbname": os.getenv("DB_NAME", "financial_data"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "postgres"),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432")
        }

    def create_tables(self):
        """Create necessary database tables"""
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        try:
            # Users table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    age INTEGER,
                    occupation VARCHAR(100),
                    income_bracket VARCHAR(50)
                )
            """)
            
            # Accounts table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(user_id),
                    account_type VARCHAR(50),
                    institution VARCHAR(100),
                    current_balance DECIMAL(15,2)
                )
            """)
            
            # Transactions table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id SERIAL PRIMARY KEY,
                    account_id INTEGER REFERENCES accounts(account_id),
                    date TIMESTAMP,
                    amount DECIMAL(15,2),
                    category VARCHAR(100),
                    merchant_name VARCHAR(200),
                    transaction_type VARCHAR(50)
                )
            """)
            
            conn.commit()
        
        finally:
            cur.close()
            conn.close()

    def generate_user_profile(self, index: int) -> Dict:
        """Generate a realistic user profile using Claude"""
        prompt = f"""Generate a realistic user profile for financial data analysis with the following details in JSON format:
        - A realistic full name
        - Age (between 25-75)
        - Occupation
        - Income bracket (one of: "25k-50k", "50k-75k", "75k-100k", "100k-150k", "150k+")
        Make it realistic and varied."""

        response = self.client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=300,
            temperature=0.7,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return json.loads(response.content)

    def generate_accounts(self, user_profile: Dict) -> List[Dict]:
        """Generate realistic account data using Claude"""
        income = user_profile["income_bracket"]
        prompt = f"""Generate realistic bank account data for a person with income {income} in JSON format.
        Include 2-4 accounts with:
        - account_type (checking, savings, credit_card, investment)
        - institution (use realistic bank names)
        - current_balance (make it realistic based on income and account type)
        Return as a list of account objects."""

        response = self.client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=500,
            temperature=0.7,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return json.loads(response.content)

    def generate_transactions(self, account: Dict, user_profile: Dict) -> List[Dict]:
        """Generate 2 years of realistic transactions using Claude"""
        prompt = f"""Generate 2 years of realistic transactions for a {account['account_type']} account 
        with current balance ${account['current_balance']} for a person with income {user_profile['income_bracket']}.
        Include:
        - date (between 2022-01-01 and 2023-12-31)
        - amount
        - category
        - merchant_name
        - transaction_type (debit/credit)
        Make transactions realistic based on account type and balance. Return as JSON list."""

        response = self.client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1000,
            temperature=0.7,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return json.loads(response.content)

    def insert_data(self, user_data: Dict, accounts_data: List[Dict], transactions_data: List[Dict]):
        """Insert generated data into PostgreSQL database"""
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        try:
            # Insert user
            cur.execute(
                """INSERT INTO users (name, age, occupation, income_bracket) 
                   VALUES (%s, %s, %s, %s) RETURNING user_id""",
                (user_data["name"], user_data["age"], user_data["occupation"], 
                 user_data["income_bracket"])
            )
            user_id = cur.fetchone()[0]
            
            # Insert accounts
            for account in accounts_data:
                cur.execute(
                    """INSERT INTO accounts (user_id, account_type, institution, current_balance) 
                       VALUES (%s, %s, %s, %s) RETURNING account_id""",
                    (user_id, account["account_type"], account["institution"], 
                     account["current_balance"])
                )
                account_id = cur.fetchone()[0]
                
                # Insert transactions for this account
                execute_values(
                    cur,
                    """INSERT INTO transactions 
                       (account_id, date, amount, category, merchant_name, transaction_type)
                       VALUES %s""",
                    [(account_id, txn["date"], txn["amount"], txn["category"],
                      txn["merchant_name"], txn["transaction_type"]) 
                     for txn in transactions_data]
                )
            
            conn.commit()
            
        finally:
            cur.close()
            conn.close()

    def generate_all_data(self, num_users: int = 100):
        """Generate and store data for specified number of users"""
        self.create_tables()
        
        for i in range(num_users):
            print(f"Generating data for user {i+1}/{num_users}")
            
            user_profile = self.generate_user_profile(i)
            accounts = self.generate_accounts(user_profile)
            
            all_transactions = []
            for account in accounts:
                transactions = self.generate_transactions(account, user_profile)
                all_transactions.extend(transactions)
            
            self.insert_data(user_profile, accounts, all_transactions)
            print(f"Completed user {i+1}")

if __name__ == "__main__":
    generator = FinancialDataGenerator()
    generator.generate_all_data(100)
