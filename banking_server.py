
import grpc
from concurrent import futures
from datetime import datetime, timezone
from threading import RLock
from uuid import uuid4

import banking_pb2
import banking_pb2_grpc


class BankData:
    def __init__(self):
        # All monetary values are stored in paise.
        self.accounts = {
            "101": 1000000,  # Alice: Rs. 10,000
            "102": 500000,   # Bob: Rs. 5,000
            "103": 800000,   # Charlie: Rs. 8,000
        }
        self.transactions = []
        self.lock = RLock()


class AccountManagementService(
    banking_pb2_grpc.AccountManagementServiceServicer
):
    def __init__(self, bank):
        self.bank = bank

    def getBalance(self, request, context):
        with self.bank.lock:
            if request.user_id not in self.bank.accounts:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    "Account does not exist."
                )

            return banking_pb2.BalanceResponse(
                user_id=request.user_id,
                balance_paise=self.bank.accounts[request.user_id]
            )

    def updateBalance(self, request, context):
        with self.bank.lock:
            if request.user_id not in self.bank.accounts:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    "Account does not exist."
                )

            if request.new_balance_paise < 0:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Account balance cannot be negative."
                )

            self.bank.accounts[request.user_id] = (
                request.new_balance_paise
            )

            return banking_pb2.UpdateBalanceResponse(
                success=True,
                message="Account balance updated successfully.",
                updated_balance_paise=request.new_balance_paise
            )


class TransactionService(
    banking_pb2_grpc.TransactionServiceServicer
):
    def __init__(self, bank):
        self.bank = bank

    def initiateTransfer(self, request, context):
        with self.bank.lock:
            sender = request.from_user_id
            receiver = request.to_user_id
            amount = request.amount_paise

            if amount <= 0:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Transfer amount must be greater than zero."
                )

            if sender == receiver:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Cannot transfer money to the same account."
                )

            if sender not in self.bank.accounts:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    "Sender account does not exist."
                )

            if receiver not in self.bank.accounts:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    "Receiver account does not exist."
                )

            if self.bank.accounts[sender] < amount:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "Insufficient funds."
                )

            # Both balance updates are protected by the same lock.
            self.bank.accounts[sender] -= amount
            self.bank.accounts[receiver] += amount

            transaction_id = str(uuid4())

            transaction = banking_pb2.Transaction(
                transaction_id=transaction_id,
                from_user_id=sender,
                to_user_id=receiver,
                amount_paise=amount,
                timestamp=datetime.now(timezone.utc).isoformat()
            )

            self.bank.transactions.append(transaction)

            return banking_pb2.TransferResponse(
                success=True,
                message="Money transferred successfully.",
                transaction_id=transaction_id
            )

    def getTransactionHistory(self, request, context):
        with self.bank.lock:
            if request.user_id not in self.bank.accounts:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    "Account does not exist."
                )

            history = [
                transaction
                for transaction in self.bank.transactions
                if (
                    transaction.from_user_id == request.user_id
                    or transaction.to_user_id == request.user_id
                )
            ]

            return banking_pb2.TransactionHistoryResponse(
                transactions=history
            )


def serve():
    bank = BankData()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    banking_pb2_grpc.add_AccountManagementServiceServicer_to_server(
        AccountManagementService(bank),
        server
    )

    banking_pb2_grpc.add_TransactionServiceServicer_to_server(
        TransactionService(bank),
        server
    )

    address = "127.0.0.1:50051"
    bound_port = server.add_insecure_port(address)

    if bound_port == 0:
        raise RuntimeError(f"Failed to bind server to {address}")

    server.start()

    print("Banking gRPC Server started successfully!")
    print(f"Listening on {address}")
    print("Press Ctrl+C to stop the server.")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=5).wait()
        print("\nBanking gRPC Server stopped.")


if __name__ == "__main__":
    serve()
