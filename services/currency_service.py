"""
Currency Conversion Service
Handles conversion between CFA/XOF and EUR with live exchange rates
"""

import requests
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import json
import os

logger = logging.getLogger(__name__)

class CurrencyService:
    """Service for currency conversion with live exchange rates."""
    
    def __init__(self):
        self.base_currency = 'XOF'  # West African CFA franc
        self.target_currency = 'EUR'  # Euro
        self.cache_file = 'instance/exchange_rates.json'
        self.cache_duration = timedelta(hours=1)  # Cache for 1 hour
        self.last_update = None
        self.cached_rates = {}
        
        # Ensure instance directory exists
        os.makedirs('instance', exist_ok=True)
        
        # Load cached rates
        self._load_cached_rates()
    
    def _load_cached_rates(self):
        """Load cached exchange rates from file."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    data = json.load(f)
                    self.cached_rates = data.get('rates', {})
                    self.last_update = datetime.fromisoformat(data.get('last_update', '2020-01-01'))
            else:
                self.cached_rates = {}
                self.last_update = datetime(2020, 1, 1)
        except Exception as e:
            logger.warning(f"Error loading cached rates: {e}")
            self.cached_rates = {}
            self.last_update = datetime(2020, 1, 1)
    
    def _save_cached_rates(self):
        """Save exchange rates to cache file."""
        try:
            data = {
                'rates': self.cached_rates,
                'last_update': self.last_update.isoformat()
            }
            with open(self.cache_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Error saving cached rates: {e}")
    
    def _is_cache_valid(self) -> bool:
        """Check if cached rates are still valid."""
        if not self.cached_rates or not self.last_update:
            return False
        return datetime.now() - self.last_update < self.cache_duration
    
    def _fetch_live_rates(self) -> bool:
        """Fetch live exchange rates from free API."""
        try:
            # Try multiple free exchange rate APIs
            apis = [
                {
                    'url': f'https://api.exchangerate-api.com/v4/latest/{self.base_currency}',
                    'parser': lambda data: data.get('rates', {})
                },
                {
                    'url': f'https://api.fixer.io/latest?base={self.base_currency}&access_key=YOUR_KEY',
                    'parser': lambda data: data.get('rates', {})
                },
                {
                    'url': f'https://open.er-api.com/v6/latest/{self.base_currency}',
                    'parser': lambda data: data.get('rates', {})
                }
            ]
            
            for api in apis:
                try:
                    response = requests.get(api['url'], timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        rates = api['parser'](data)
                        
                        if self.target_currency in rates:
                            self.cached_rates[f'{self.base_currency}_to_{self.target_currency}'] = rates[self.target_currency]
                            self.last_update = datetime.now()
                            self._save_cached_rates()
                            logger.info(f"Successfully updated exchange rate: 1 {self.base_currency} = {rates[self.target_currency]} {self.target_currency}")
                            return True
                            
                except Exception as e:
                    logger.warning(f"Failed to fetch from {api['url']}: {e}")
                    continue
            
            # If all APIs fail, try using a fixed rate as fallback
            logger.warning("All exchange rate APIs failed, using fallback rate")
            return self._use_fallback_rate()
            
        except Exception as e:
            logger.error(f"Error fetching live rates: {e}")
            return self._use_fallback_rate()
    
    def _use_fallback_rate(self) -> bool:
        """Use a fallback exchange rate when APIs are unavailable."""
        try:
            # Fixed rate as of 2024 (approximate): 1 EUR = 655.957 XOF
            # So 1 XOF = 0.001525 EUR (approximately)
            fallback_rate = 0.001525
            
            self.cached_rates[f'{self.base_currency}_to_{self.target_currency}'] = fallback_rate
            self.last_update = datetime.now()
            self._save_cached_rates()
            
            logger.warning(f"Using fallback exchange rate: 1 {self.base_currency} = {fallback_rate} {self.target_currency}")
            return True
            
        except Exception as e:
            logger.error(f"Error setting fallback rate: {e}")
            return False
    
    def get_exchange_rate(self, force_update: bool = False) -> Optional[float]:
        """Get current exchange rate from XOF to EUR."""
        try:
            # Check if we need to update rates
            if force_update or not self._is_cache_valid():
                if not self._fetch_live_rates():
                    logger.error("Failed to fetch exchange rates")
                    
            # Return cached rate
            rate_key = f'{self.base_currency}_to_{self.target_currency}'
            return self.cached_rates.get(rate_key)
            
        except Exception as e:
            logger.error(f"Error getting exchange rate: {e}")
            return None
    
    def convert_to_eur(self, xof_amount: float, force_update: bool = False) -> Optional[float]:
        """Convert XOF amount to EUR."""
        try:
            if xof_amount is None or xof_amount == 0:
                return 0.0
                
            rate = self.get_exchange_rate(force_update)
            if rate is None:
                logger.warning("No exchange rate available, returning original amount")
                return xof_amount
            
            eur_amount = xof_amount * rate
            return round(eur_amount, 2)
            
        except Exception as e:
            logger.error(f"Error converting {xof_amount} XOF to EUR: {e}")
            return xof_amount  # Return original amount if conversion fails
    
    def convert_from_eur(self, eur_amount: float, force_update: bool = False) -> Optional[float]:
        """Convert EUR amount to XOF."""
        try:
            if eur_amount is None or eur_amount == 0:
                return 0.0
                
            rate = self.get_exchange_rate(force_update)
            if rate is None:
                logger.warning("No exchange rate available, returning original amount")
                return eur_amount
            
            xof_amount = eur_amount / rate
            return round(xof_amount, 2)
            
        except Exception as e:
            logger.error(f"Error converting {eur_amount} EUR to XOF: {e}")
            return eur_amount  # Return original amount if conversion fails
    
    def get_currency_info(self) -> Dict[str, Any]:
        """Get currency conversion information."""
        rate = self.get_exchange_rate()
        
        return {
            'base_currency': self.base_currency,
            'target_currency': self.target_currency,
            'exchange_rate': rate,
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'cache_valid': self._is_cache_valid(),
            'conversion_info': f"1 {self.base_currency} = {rate:.6f} {self.target_currency}" if rate else "No rate available"
        }
    
    def format_currency(self, amount: float, currency: str = 'XOF', show_eur: bool = True) -> str:
        """Format currency amount with optional EUR conversion."""
        try:
            if currency == 'XOF':
                formatted = f"{amount:,.0f} CFA"
                if show_eur and amount > 0:
                    eur_amount = self.convert_to_eur(amount)
                    if eur_amount and eur_amount != amount:
                        formatted += f" (€{eur_amount:,.2f})"
                return formatted
            elif currency == 'EUR':
                return f"€{amount:,.2f}"
            else:
                return f"{amount:,.2f} {currency}"
                
        except Exception as e:
            logger.error(f"Error formatting currency: {e}")
            return f"{amount} {currency}"

# Global instance
currency_service = CurrencyService()
