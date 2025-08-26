from typing import Any, Dict


class Handler:
    def __call__(self, transaction: Dict, account: Dict) -> Any:
        self.process_transaction({"transaction": transaction, "account": account})

    def process_transaction(self, data: Dict) -> Any:
        # Implement your transaction processing logic here
        raise NotImplementedError("Subclasses must implement this method.")
