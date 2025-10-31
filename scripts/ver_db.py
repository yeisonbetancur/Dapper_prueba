#!/usr/bin/env python3
"""
Script para visualizar todas las regulaciones en la base de datos
Uso: python ver_db.py
"""

import sys
import os

# Añadir el directorio src al path
sys.path.insert(0, '/opt/airflow/src')
from db import DatabaseManager
from tabulate import tabulate


def mostrar_todas_regulaciones():
    """Muestra todas las regulaciones en la base de datos"""
    
    db_manager = DatabaseManager()
    
    if not db_manager.connect():
        print("❌ Error conectando a la base de datos")
        return
    
    try:
        # Query para obtener todas las regulaciones
        query = """
            SELECT id, title, entity, created_at, is_active, rtype_id, external_link
            FROM regulations 
            ORDER BY id DESC
        """
        
        result = db_manager.execute_query(query)
        
        if not result:
            print("\n⚠️  No hay regulaciones en la base de datos\n")
            return
        
        # Preparar datos para mostrar
        headers = ['ID', 'Título', 'Entidad', 'Fecha Creación', 'Activo', 'Tipo', 'Link']
        rows = []
        
        for row in result:
            reg_id, title, entity, created_at, is_active, rtype_id, external_link = row
            
            # Truncar título si es muy largo
            title_short = title[:50] + '...' if len(title) > 50 else title
            
            # Formatear fecha
            fecha_str = created_at.strftime('%Y-%m-%d') if created_at else 'N/A'
            
            # Formatear activo
            activo_str = '✓' if is_active else '✗'
            
            # Truncar link
            link_short = external_link[:40] + '...' if external_link and len(external_link) > 40 else (external_link or 'N/A')
            
            rows.append([reg_id, title_short, entity, fecha_str, activo_str, rtype_id, link_short])
        
        # Mostrar resultados
        print("\n" + "=" * 150)
        print(f"📋 TODAS LAS REGULACIONES EN LA BASE DE DATOS (Total: {len(rows)})")
        print("=" * 150 + "\n")
        print(tabulate(rows, headers=headers, tablefmt='grid'))
        print("\n" + "=" * 150)
        print(f"Total de registros: {len(rows)}")
        print("=" * 150 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error consultando regulaciones: {e}\n")
        import traceback
        traceback.print_exc()
        
    finally:
        db_manager.close()


if __name__ == "__main__":
    mostrar_todas_regulaciones()