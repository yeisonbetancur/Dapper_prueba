import re
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional


class DataValidator:
    """Valida filas de DataFrame contra reglas JSON configurables."""
    
    VALID_TYPES = {"int", "float", "date", "str"}
    
    def __init__(self, rules_path: str):
        """
        Carga y valida reglas desde archivo JSON.
        
        Args:
            rules_path: Ruta al archivo JSON que contiene las reglas de validación
            
        Raises:
            ValueError: Si la estructura del archivo de reglas es inválida
        """
        with open(rules_path, "r", encoding="utf-8") as f:
            self.rules = json.load(f)
        
        self._validate_rules_structure()
    
    def _validate_rules_structure(self) -> None:
        """Valida que las reglas cargadas tengan la estructura correcta."""
        if not isinstance(self.rules, dict):
            raise ValueError("Las reglas tienen un formato incorrecto")
        
        for entity, fields in self.rules.items():
            if not isinstance(fields, dict):
                raise ValueError(f"Reglas para la entidad '{entity}' tienen formato incorrecto")
            
            for field, rule in fields.items():
                if not isinstance(rule, dict):
                    raise ValueError(f"Reglas para el campo {entity}.{field} tienen formato incorrecto")
                
                # Validar tipo si está especificado
                if "type" in rule and rule["type"] not in self.VALID_TYPES:
                    raise ValueError(
                        f"Tipo invalido '{rule['type']}' para {entity}.{field}. "
                        f"Tipos Validos: {self.VALID_TYPES}"
                    )
                
                # Validar regex si está especificado
                if "regex" in rule:
                    try:
                        re.compile(rule["regex"])
                    except re.error as e:
                        raise ValueError(
                            f"Regex invalido para {entity}.{field}: {str(e)}"
                        )
    
    def _validate_value(self, value: Any, rule: Dict, field: str) -> Tuple[bool, Any, Optional[str]]:
        """
        Valida un valor individual contra una regla.
        
        Args:
            value: El valor a validar
            rule: Diccionario que contiene las reglas de validación
            field: Nombre del campo (para mensajes de error)
            
        Returns:
            Tupla de (es_valido, valor_validado, mensaje_error)
        """
        # Manejar valores nulos
        if pd.isna(value) or value is None:
            return True, None, None
        
        validated_value = value
        
        # Validar y convertir tipo
        expected_type = rule.get("type")
        if expected_type:
            try:
                if expected_type == "int":
                    validated_value = int(value)
                elif expected_type == "float":
                    validated_value = float(value)
                elif expected_type == "date":
                    validated_value = datetime.strptime(str(value), "%Y-%m-%d")
                elif expected_type == "str":
                    validated_value = str(value).strip()
            except (ValueError, TypeError) as e:
                return False, None, f"Validacion del tipo fallida para {validated_value}: expected {expected_type}"
        
        # Validar regex (solo para valores de texto)
        regex = rule.get("regex")
        if regex:
            str_value = str(validated_value) if not isinstance(validated_value, datetime) else validated_value.strftime("%Y-%m-%d")
            if not re.match(regex, str_value):
                return False, None, f"Validacion de regex fallida: {regex}"
        
        return True, validated_value, None
    
    def validate(self, df: pd.DataFrame, entity: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Valida DataFrame según las reglas de la entidad.
        
        Args:
            df: DataFrame a validar
            entity: Nombre de entidad correspondiente a las reglas
            
        Returns:
            Tupla de (dataframe_validado, reporte_validacion)
            
        El reporte de validación contiene:
            - total_input_rows: Número de filas de entrada
            - total_valid_rows: Número de filas válidas después de validación
            - total_dropped_rows: Número de filas descartadas
            - discarded_rows: Lista de detalles de filas descartadas
            - invalid_by_field: Conteo de valores inválidos por campo
        """
        # Verificar si la entidad existe
        if entity not in self.rules:
            print(f"⚠️ No hay reglas definidas para la entidad '{entity}', se devuelve sin cambios.")
            return df, {
                'total_input_rows': len(df),
                'total_valid_rows': len(df),
                'total_dropped_rows': 0,
                'discarded_rows': [],
                'invalid_by_field': {}
            }
        
        rules = self.rules[entity]
        
        # Verificar columnas requeridas faltantes
        missing_required = []
        for field, rule in rules.items():
            if rule.get("required", False) and field not in df.columns:
                missing_required.append(field)
        
        if missing_required:
            raise ValueError(f"Faltan columnas requeridas para la entidad '{entity}': {missing_required}")
        
        # Inicializar seguimiento
        validated_data = []
        discarded_rows = []
        invalid_by_field = {field: 0 for field in rules.keys()}
        total_input = len(df)
        
        # Validar cada fila
        for idx, row in df.iterrows():
            new_row = {}
            discard_row = False
            discard_reason = None
            
            for field, rule in rules.items():
                value = row.get(field, None)
                is_valid, validated_value, error_msg = self._validate_value(value, rule, field)
                
                # Rastrear campos inválidos
                if not is_valid:
                    invalid_by_field[field] += 1
                
                # Verificar restricción de campo requerido
                if rule.get("required", False) and (not is_valid or validated_value is None):
                    discard_row = True
                    discard_reason = f"Campo requerido '{field}' inválido o faltante"
                    if error_msg:
                        discard_reason += f" ({error_msg})"
                    break
                
                new_row[field] = validated_value
            
            # Mantener o descartar fila
            if discard_row:
                discarded_rows.append({
                    'original_index': int(idx),
                    'reason': discard_reason,
                    'row_data': row.to_dict()
                })
            else:
                validated_data.append(new_row)
        
        # Crear DataFrame validado
        clean_df = pd.DataFrame(validated_data) if validated_data else pd.DataFrame(columns=df.columns)
        
        # Construir reporte de validación
        validation_report = {
            'total_input_rows': total_input,
            'total_valid_rows': len(clean_df),
            'total_dropped_rows': len(discarded_rows),
            'discarded_rows': discarded_rows,
            'invalid_by_field': invalid_by_field
        }
        
        # Imprimir resumen
        print(f"✅ Validación completada para la entidad '{entity}':")
        print(f"   • Filas de entrada: {total_input}")
        print(f"   • Filas válidas: {len(clean_df)}")
        print(f"   • Filas descartadas: {len(discarded_rows)}")
        if invalid_by_field:
            print(f"   • Valores inválidos por campo: {invalid_by_field}")
        
        return clean_df, validation_report
    
    def get_entity_rules(self, entity: str) -> Optional[Dict]:
        """Obtiene las reglas para una entidad específica."""
        return self.rules.get(entity)
    
    def list_entities(self) -> List[str]:
        """Lista todas las entidades disponibles."""
        return list(self.rules.keys())


# Ejemplo de uso:
if __name__ == "__main__":
    # Contenido de ejemplo del archivo de reglas:
    # {
    #   "users": {
    #     "user_id": {"type": "int", "required": true},
    #     "email": {"type": "str", "regex": "^[\\w.-]+@[\\w.-]+\\.\\w+$", "required": true},
    #     "age": {"type": "int"},
    #     "created_at": {"type": "date", "required": true}
    #   }
    # }
    
    # Crear datos de ejemplo
    sample_df = pd.DataFrame({
        'user_id': [1, 2, 'invalid', 4],
        'email': ['test@example.com', 'bad-email', 'user@test.com', None],
        'age': [25, 30, 35, 40],
        'created_at': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04']
    })
    
    # Validar
    validator = DataValidator('rules.json')
    clean_df, report = validator.validate(sample_df, 'users')
    
    print("\nReporte de Validación:")
    print(json.dumps(report, indent=2, default=str))

def run_validation(
    df: pd.DataFrame, 
    entity: str, 
    rules_path: str = 'config/validation_rules.json'
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Función de entrada para validación en DAG.
    
    Args:
        df: DataFrame a validar
        entity: Nombre de la entidad a validar
        rules_path: Ruta al archivo de reglas JSON
        
    Returns:
        Tupla de (dataframe_validado, reporte_validacion)
    """
    validator = DataValidator(rules_path)
    return validator.validate(df, entity)