#!/usr/bin/env python3
"""
Script para generar datos de ejemplo para el pipeline de datos
NO incluye datos reales - solo datos de prueba para desarrollo
"""

import pandas as pd
import random
import os
from datetime import datetime, timedelta

def create_directories():
    """Crear directorios necesarios"""
    directories = [
        'data/supabase',
        'data/rds'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

def generate_bancos_data():
    """Generar datos de ejemplo para bancos"""
    bancos_data = []
    
    sample_banks = [
        {"issuer": "Banco Nacional", "name": "Banco Nacional S.A."},
        {"issuer": "Banco Popular", "name": "Banco Popular S.A."},
        {"issuer": "Banco de Bogotá", "name": "Banco de Bogotá S.A."},
        {"issuer": "Banco Davivienda", "name": "Banco Davivienda S.A."},
        {"issuer": "Banco Colpatria", "name": "Banco Colpatria S.A."}
    ]
    
    for i, bank in enumerate(sample_banks, 1):
        bancos_data.append({
            'bankid': i,
            'issuer': bank['issuer'],
            'name': bank['name'],
            'creation_date_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'activo',
            'source_database': 'SUPABASE',
            'export_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return bancos_data

def generate_cardholders_data():
    """Generar datos de ejemplo para titulares de tarjetas"""
    cardholders_data = []
    
    sample_names = [
        "Juan Pérez", "María García", "Carlos López", "Ana Martínez", "Luis Rodríguez",
        "Carmen Sánchez", "Pedro González", "Laura Fernández", "Diego Ramírez", "Sofia Torres"
    ]
    
    for i, name in enumerate(sample_names, 1):
        cardholders_data.append({
            'cardholderid': i,
            'name': name,
            'email': f"{name.lower().replace(' ', '.')}@email.com",
            'phone': f"+57-300-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            'creation_date_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'activo',
            'source_database': 'SUPABASE',
            'export_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return cardholders_data

def generate_programas_data():
    """Generar datos de ejemplo para programas"""
    programas_data = []
    
    sample_programs = [
        {"name": "Programa Puntos Oro", "description": "Programa de lealtad con puntos dorados"},
        {"name": "Programa Puntos Plata", "description": "Programa de lealtad con puntos plateados"},
        {"name": "Programa Puntos Bronce", "description": "Programa de lealtad con puntos bronce"}
    ]
    
    for i, program in enumerate(sample_programs, 1):
        start_date = datetime.now().strftime('%Y-%m-%d')
        end_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
        
        programas_data.append({
            'programid': i,
            'name': program['name'],
            'description': program['description'],
            'start_date': start_date,
            'end_date': end_date,
            'status': 'activo',
            'source_database': 'SUPABASE',
            'export_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return programas_data

def generate_tarjetas_data():
    """Generar datos de ejemplo para tarjetas de puntos"""
    tarjetas_data = []
    
    for i in range(1, 11):  # 10 tarjetas
        tarjetas_data.append({
            'tarjetaid': i,
            'cardholderid': i,
            'programid': random.randint(1, 3),
            'card_number': f"****-****-****-{random.randint(1000, 9999)}",
            'points_balance': random.randint(100, 5000),
            'creation_date_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'activo',
            'source_database': 'SUPABASE',
            'export_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return tarjetas_data

def generate_usuarios_data():
    """Generar datos de ejemplo para usuarios"""
    usuarios_data = []
    
    sample_users = [
        {"username": "admin", "email": "admin@empresa.com", "role": "administrador"},
        {"username": "analyst", "email": "analyst@empresa.com", "role": "analista"},
        {"username": "operator", "email": "operator@empresa.com", "role": "operador"}
    ]
    
    for i, user in enumerate(sample_users, 1):
        usuarios_data.append({
            'userid': i,
            'username': user['username'],
            'email': user['email'],
            'role': user['role'],
            'creation_date_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'activo',
            'source_database': 'SUPABASE',
            'export_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return usuarios_data

def save_data_to_csv(data, filename, database_type):
    """Guardar datos en archivo CSV"""
    df = pd.DataFrame(data)
    filepath = f'data/{database_type}/{filename}'
    df.to_csv(filepath, index=False)
    print(f"Generated: {filepath} ({len(data)} records)")

def main():
    """Función principal"""
    print("Generating sample data for PostgreSQL to Snowflake Data Pipeline")
    print("=" * 60)
    
    # Crear directorios
    create_directories()
    
    # Generar datos
    print("\nGenerating data...")
    
    # Datos para Supabase
    save_data_to_csv(generate_bancos_data(), 'banco.csv', 'supabase')
    save_data_to_csv(generate_cardholders_data(), 'card_holder.csv', 'supabase')
    save_data_to_csv(generate_programas_data(), 'programa.csv', 'supabase')
    save_data_to_csv(generate_tarjetas_data(), 'tarjeta_puntos.csv', 'supabase')
    save_data_to_csv(generate_usuarios_data(), 'usuario.csv', 'supabase')
    
    # Datos para RDS (mismo formato, diferentes IDs)
    rds_bancos = generate_bancos_data()
    for bank in rds_bancos:
        bank['bankid'] += 100  # IDs diferentes para RDS
        bank['source_database'] = 'RDS'
    
    rds_cardholders = generate_cardholders_data()
    for holder in rds_cardholders:
        holder['cardholderid'] += 100
        holder['source_database'] = 'RDS'
    
    rds_programas = generate_programas_data()
    for program in rds_programas:
        program['programid'] += 100
        program['source_database'] = 'RDS'
    
    rds_tarjetas = generate_tarjetas_data()
    for tarjeta in rds_tarjetas:
        tarjeta['tarjetaid'] += 100
        tarjeta['cardholderid'] += 100
        tarjeta['programid'] += 100
        tarjeta['source_database'] = 'RDS'
    
    rds_usuarios = generate_usuarios_data()
    for user in rds_usuarios:
        user['userid'] += 100
        user['source_database'] = 'RDS'
    
    save_data_to_csv(rds_bancos, 'banco.csv', 'rds')
    save_data_to_csv(rds_cardholders, 'card_holder.csv', 'rds')
    save_data_to_csv(rds_programas, 'programa.csv', 'rds')
    save_data_to_csv(rds_tarjetas, 'tarjeta_puntos.csv', 'rds')
    save_data_to_csv(rds_usuarios, 'usuario.csv', 'rds')
    
    print("\n" + "=" * 60)
    print("Sample data generation completed successfully!")
    print("You can now run the data pipeline DAGs.")
    print("\nIMPORTANT: These are sample data only. Replace with your actual data.")

if __name__ == "__main__":
    main()
