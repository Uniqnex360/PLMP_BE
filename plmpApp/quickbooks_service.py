from rest_framework import exceptions
import os
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from functools import wraps
from intuitlib.client import AuthClient
from intuitlib.enums import Scopes
from intuitlib.exceptions import AuthClientError
from quickbooks import QuickBooks
from quickbooks.objects.customer import Customer
from quickbooks.objects.vendor import Vendor
from quickbooks.objects.item import Item
from quickbooks.objects.invoice import Invoice
from quickbooks.objects.payment import Payment
from quickbooks.objects.bill import Bill
from quickbooks.objects.purchaseorder import PurchaseOrder
from quickbooks.objects.account import Account
from quickbooks.objects import CompanyInfo
from quickbooks.exceptions import QuickbooksException
logger = logging.getLogger(__name__)
class QuickBooksService:
    def __init__(self, client_id=None):
        self.client_id_param = client_id
        self.qb_client_id = os.getenv('QUICKBOOKS_CLIENT_ID')
        self.qb_client_secret = os.getenv('QUICKBOOKS_CLIENT_SECRET')
        self.redirect_uri = os.getenv('QUICKBOOKS_REDIRECT_URI')
        self.environment = os.getenv('QUICKBOOKS_ENVIRONMENT', 'sandbox')
        self.auth_client = AuthClient(
            client_id=self.qb_client_id,
            client_secret=self.qb_client_secret,
            redirect_uri=self.redirect_uri,
            environment=self.environment
        )
    def get_authorization_url(self):
        scopes = [
            Scopes.ACCOUNTING,
            Scopes.PAYMENT,
            Scopes.OPENID,
            Scopes.PROFILE,
            Scopes.EMAIL,
        ]
        return self.auth_client.get_authorization_url(scopes=scopes)
    def handle_callback(self, auth_code, realm_id):
        from .models import QuickBooksToken, client
        try:
            self.auth_client.get_bearer_token(auth_code, realm_id=realm_id)
            expires_at = datetime.now() + timedelta(seconds=self.auth_client.expires_in)
            refresh_expires_at = datetime.now() + timedelta(days=100)
            client_obj = None
            if self.client_id_param:
                client_obj = client.objects(
                    id=ObjectId(self.client_id_param)).first()
            existing_token = QuickBooksToken.objects(realm_id=realm_id).first()
            if existing_token:
                existing_token.access_token = self.auth_client.access_token
                existing_token.refresh_token = self.auth_client.refresh_token
                existing_token.expires_at = expires_at
                existing_token.refresh_token_expires_at = refresh_expires_at
                existing_token.save()
            else:
                QuickBooksToken(
                    access_token=self.auth_client.access_token,
                    refresh_token=self.auth_client.refresh_token,
                    realm_id=realm_id,
                    expires_at=expires_at,
                    refresh_token_expires_at=refresh_expires_at,
                    client_id=client_obj
                ).save()
            return {'success': True, 'realm_id': realm_id, 'message': 'Connected successfully'}
        except Exception as e:
            logger.error(f"Error handling callback: {e}")
            return {'success': False, 'error': str(e)}
    def refresh_access_token(self, realm_id):
        from .models import QuickBooksToken
        try:
            token = QuickBooksToken.objects(realm_id=realm_id).first()
            if not token:
                return {'success': False, 'error': 'No token found'}
            self.auth_client.refresh(refresh_token=token.refresh_token)
            token.access_token = self.auth_client.access_token
            token.refresh_token = self.auth_client.refresh_token
            token.expires_at = datetime.now() + timedelta(seconds=self.auth_client.expires_in)
            token.save()
            return {'success': True, 'message': 'Token refreshed'}
        except Exception as e:
            logger.error(f"Error refreshing token: {e}")
            return {'success': False, 'error': str(e)}
    def get_qb_client(self, realm_id):
        from .models import QuickBooksToken
        token = QuickBooksToken.objects(realm_id=realm_id).first()
        if not token:
            raise Exception("QuickBooks not connected")
        if token.is_token_expired():
            refresh_result = self.refresh_access_token(realm_id)
            if not refresh_result['success']:
                raise Exception(
                    f"Failed to refresh token: {refresh_result['error']}")
            token.reload()
        self.auth_client.access_token = token.access_token
        self.auth_client.refresh_token = token.refresh_token
        self.auth_client.realm_id = realm_id
        return QuickBooks(
            auth_client=self.auth_client,
            refresh_token=token.refresh_token,
            company_id=realm_id,
            minorversion=65
        )
    def disconnect(self, realm_id):
        from .models import QuickBooksToken
        try:
            token = QuickBooksToken.objects(realm_id=realm_id).first()
            if token:
                self.auth_client.revoke(token=token.refresh_token)
                token.delete()
            return {'success': True, 'message': 'Disconnected successfully'}
        except Exception as e:
            logger.error(f"Error disconnecting: {e}")
            return {'success': False, 'error': str(e)}
    def get_connection_status(self):
        from .models import QuickBooksToken
        try:
            tokens = QuickBooksToken.objects()
            connections = []
            for token in tokens:
                try:
                    qb_client = self.get_qb_client(token.realm_id)
                    company_info = CompanyInfo.all(qb=qb_client)[0]
                    connections.append({
                        'realm_id': token.realm_id,
                        'connected': True,
                        'token_expired': token.is_token_expired(),
                        'company': {
                            'id': company_info.Id,
                            'name': company_info.CompanyName,
                            'legal_name': company_info.LegalName,
                            'country': company_info.Country,
                            'email': company_info.Email.Address if company_info.Email else None
                        },
                        'connected_at': str(token.created_at)
                    })
                except Exception as e:
                    connections.append({
                        'realm_id': token.realm_id,
                        'connected': False,
                        'error': str(e)
                    })
            return {'success': True, 'connections': connections, 'is_connected': len(connections) > 0}
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {'success': False, 'error': str(e)}
    def get_all_customers(self, realm_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            customers = Customer.all(qb=qb_client, max_results=1000)
            customer_list = []
            for c in customers:
                customer_list.append({
                    'id': c.Id,
                    'display_name': c.DisplayName,
                    'company_name': c.CompanyName,
                    'first_name': c.GivenName,
                    'last_name': c.FamilyName,
                    'email': c.PrimaryEmailAddr.Address if c.PrimaryEmailAddr else None,
                    'phone': c.PrimaryPhone.FreeFormNumber if c.PrimaryPhone else None,
                    'mobile': c.Mobile.FreeFormNumber if c.Mobile else None,
                    'billing_address': self._format_address(c.BillAddr),
                    'shipping_address': self._format_address(c.ShipAddr),
                    'payment_terms': c.SalesTermRef.name if c.SalesTermRef else None,
                    'balance': float(c.Balance) if c.Balance else 0,
                    'opening_balance': float(c.OpenBalance) if hasattr(c, 'OpenBalance') and c.OpenBalance else 0,
                    'notes': c.Notes,
                    'tax_exempt': c.Taxable if hasattr(c, 'Taxable') else None,
                    'is_active': c.Active,
                    'created_at': c.MetaData.get('CreateTime') if c.MetaData else None,
                    'updated_at': c.MetaData.get('LastUpdatedTime') if c.MetaData else None
                })
            return {'success': True, 'customers': customer_list, 'count': len(customer_list)}
        except Exception as e:
            logger.error(f"Error fetching customers: {e}")
            return {'success': False, 'error': str(e)}
    def get_customer_details(self, realm_id, customer_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            customer = Customer.get(customer_id, qb=qb_client)
            invoices = self.get_customer_invoices(realm_id, customer_id)
            payments = self.get_customer_payments(realm_id, customer_id)
            return {
                'success': True,
                'customer': {
                    'id': customer.Id,
                    'display_name': customer.DisplayName,
                    'company_name': customer.CompanyName,
                    'email': customer.PrimaryEmailAddr.Address if customer.PrimaryEmailAddr else None,
                    'phone': customer.PrimaryPhone.FreeFormNumber if customer.PrimaryPhone else None,
                    'billing_address': self._format_address(customer.BillAddr),
                    'shipping_address': self._format_address(customer.ShipAddr),
                    'balance': float(customer.Balance) if customer.Balance else 0,
                    'is_active': customer.Active,
                    'notes': customer.Notes
                },
                'invoices': invoices.get('invoices', []),
                'payments': payments.get('payments', [])
            }
        except Exception as e:
            logger.error(f"Error fetching customer details: {e}")
            return {'success': False, 'error': str(e)}
    def create_customer(self, realm_id, customer_data):
        try:
            qb_client = self.get_qb_client(realm_id)
            customer = Customer()
            customer.DisplayName = customer_data.get('display_name')
            customer.CompanyName = customer_data.get('company_name')
            customer.GivenName = customer_data.get('first_name')
            customer.FamilyName = customer_data.get('last_name')
            if customer_data.get('email'):
                from quickbooks.objects.base import EmailAddress
                customer.PrimaryEmailAddr = EmailAddress()
                customer.PrimaryEmailAddr.Address = customer_data.get('email')
            if customer_data.get('phone'):
                from quickbooks.objects.base import PhoneNumber
                customer.PrimaryPhone = PhoneNumber()
                customer.PrimaryPhone.FreeFormNumber = customer_data.get(
                    'phone')
            if customer_data.get('billing_address'):
                customer.BillAddr = self._create_address(
                    customer_data.get('billing_address'))
            if customer_data.get('shipping_address'):
                customer.ShipAddr = self._create_address(
                    customer_data.get('shipping_address'))
            customer.Notes = customer_data.get('notes', '')
            customer.save(qb=qb_client)
            return {'success': True, 'customer_id': customer.Id, 'message': 'Customer created successfully'}
        except Exception as e:
            logger.error(f"Error creating customer: {e}")
            return {'success': False, 'error': str(e)}
    def get_all_invoices(self, realm_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            invoices = Invoice.all(qb=qb_client, max_results=1000)
            invoice_list = []
            for inv in invoices:
                line_items = self._parse_invoice_lines(inv.Line)
                invoice_list.append({
                    'id': inv.Id,
                    'doc_number': inv.DocNumber,
                    'customer_id': inv.CustomerRef.value if inv.CustomerRef else None,
                    'customer_name': inv.CustomerRef.name if inv.CustomerRef else None,
                    'line_items': line_items,
                    'subtotal': float(inv.TotalAmt) - float(inv.TxnTaxDetail.TotalTax if inv.TxnTaxDetail and inv.TxnTaxDetail.TotalTax else 0) if inv.TotalAmt else 0,
                    'tax_amount': float(inv.TxnTaxDetail.TotalTax) if inv.TxnTaxDetail and inv.TxnTaxDetail.TotalTax else 0,
                    'discount': self._get_discount_from_lines(inv.Line),
                    'shipping': self._get_shipping_from_lines(inv.Line),
                    'total_amount': float(inv.TotalAmt) if inv.TotalAmt else 0,
                    'balance_due': float(inv.Balance) if inv.Balance else 0,
                    'payment_status': self._get_payment_status(inv),
                    'issue_date': str(inv.TxnDate) if inv.TxnDate else None,
                    'due_date': str(inv.DueDate) if inv.DueDate else None,
                    'ship_date': str(inv.ShipDate) if inv.ShipDate else None,
                    'billing_email': inv.BillEmail.Address if inv.BillEmail else None,
                    'shipping_address': self._format_address(inv.ShipAddr),
                    'billing_address': self._format_address(inv.BillAddr),
                    'memo': inv.CustomerMemo.value if inv.CustomerMemo else None,
                    'created_at': str(inv.MetaData.CreateTime) if inv.MetaData else None
                })
            return {'success': True, 'invoices': invoice_list, 'count': len(invoice_list)}
        except Exception as e:
            logger.error(f"Error fetching invoices: {e}")
            return {'success': False, 'error': str(e)}
    def get_customer_invoices(self, realm_id, customer_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            invoices = Invoice.filter(CustomerRef=customer_id, qb=qb_client)
            invoice_list = []
            for inv in invoices:
                invoice_list.append({
                    'id': inv.Id,
                    'doc_number': inv.DocNumber,
                    'total_amount': float(inv.TotalAmt) if inv.TotalAmt else 0,
                    'balance_due': float(inv.Balance) if inv.Balance else 0,
                    'payment_status': self._get_payment_status(inv),
                    'issue_date': str(inv.TxnDate) if inv.TxnDate else None,
                    'due_date': str(inv.DueDate) if inv.DueDate else None,
                    'line_items': self._parse_invoice_lines(inv.Line)
                })
            return {'success': True, 'invoices': invoice_list}
        except Exception as e:
            logger.error(f"Error fetching customer invoices: {e}")
            return {'success': False, 'error': str(e)}
    
    def create_invoice(self, realm_id, invoice_data):
        try:
            qb_client = self.get_qb_client(realm_id)
            invoice = Invoice()
            invoice.CustomerRef = {"value": invoice_data.get('customer_id')}
            lines = []
            for item in invoice_data.get('items', []):
                line = {
                    "Amount": float(item.get('quantity', 1)) * float(item.get('unit_price', 0)),
                    "DetailType": "SalesItemLineDetail",
                    "SalesItemLineDetail": {
                        "ItemRef": {"value": item.get('item_id')},
                        "Qty": item.get('quantity', 1),
                        "UnitPrice": item.get('unit_price', 0)
                    },
                    "Description": item.get('description', '')
                }
                lines.append(line)
            invoice.Line = lines
            if invoice_data.get('due_date'):
                invoice.DueDate = invoice_data.get('due_date')
            if invoice_data.get('memo'):
                invoice.CustomerMemo = {"value": invoice_data.get('memo')}
            invoice.save(qb=qb_client)
            return {
                'success': True,
                'invoice_id': invoice.Id,
                'doc_number': invoice.DocNumber,
                'message': 'Invoice created successfully'
            }
        except Exception as e:
            logger.error(f"Error creating invoice: {e}")
            return {'success': False, 'error': str(e)}
    def get_payments_for_credit(self,payment_obj):
        if not payment_obj:
            return 0
        if not hasattr(payment_obj,'Line') or not payment_obj.Line:
            return 0
        for line in payment_obj.Line:
            if line.Amount:
                return float(line.Amount)
        return 0
    
    def get_all_payments(self, realm_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            payments = Payment.all(qb=qb_client, max_results=1000)
            payment_list = []
            for p in payments:
                linked_invoices = []
                if p.Line:
                    for line in p.Line:
                        if hasattr(line, 'LinkedTxn') and line.LinkedTxn:
                            for txn in line.LinkedTxn:
                                linked_invoices.append({
                                    'txn_id': txn.TxnId,
                                    'txn_type': txn.TxnType,
                                    'amount': float(line.Amount) if line.Amount else 0
                                })
                payment_list.append({
                    'id': p.Id,
                    'customer_id': p.CustomerRef.value if p.CustomerRef else None,
                    'customer_name': p.CustomerRef.name if p.CustomerRef else None,
                    'payment_date': str(p.TxnDate) if p.TxnDate else None,
                    'amount':self.get_payments_for_credit(p) if p.TotalAmt==0 else  float(p.TotalAmt) if p.TotalAmt else 0,
                    'payment_method': p.PaymentMethodRef.name if p.PaymentMethodRef else None,
                    'payment_ref_number': p.PaymentRefNum,
                    'deposit_account': p.DepositToAccountRef.name if p.DepositToAccountRef else None,
                    'linked_invoices': linked_invoices,
                    'memo': p.PrivateNote,
                    'created_at': str(p.MetaData.CreateTime) if p.MetaData and hasattr(p.MetaData, 'CreateTime') else
                    str(p.MetaData.get('CreateTime')) if p.MetaData and isinstance(
                        p.MetaData, dict) else None
                })
            return {'success': True, 'payments': payment_list, 'count': len(payment_list)}
        except Exception as e:
            logger.error(f"Error fetching payments: {e}")
            return {'success': False, 'error': str(e)}
    def get_customer_payments(self, realm_id, customer_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            payments = Payment.filter(CustomerRef=customer_id, qb=qb_client)
            payment_list = []
            for p in payments:
                payment_list.append({
                    'id': p.Id, 
                    'payment_date': str(p.TxnDate) if p.TxnDate else None,
                    'amount':self.get_payments_for_credit(p) if p.TotalAmt==0 else  float(p.TotalAmt) if p.TotalAmt else 0,
                    'payment_method': "Credit Applied" if p.TotalAmt == 0 and hasattr(p, 'Line') and p.Line else (p.PaymentMethodRef.name if p.PaymentMethodRef else None),
                    'payment_ref_number': p.PaymentRefNum
                })
            return {'success': True, 'payments': payment_list}
        except Exception as e:
            logger.error(f"Error fetching customer payments: {e}")
            return {'success': False, 'error': str(e)}
    def get_all_vendors(self, realm_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            vendors = Vendor.all(qb=qb_client, max_results=1000)
            vendor_list = []
            for v in vendors:
                vendor_list.append({
                    'id': v.Id,
                    'display_name': v.DisplayName,
                    'company_name': v.CompanyName,
                    'first_name': v.GivenName,
                    'last_name': v.FamilyName,
                    'email': v.PrimaryEmailAddr.Address if v.PrimaryEmailAddr else None,
                    'phone': v.PrimaryPhone.FreeFormNumber if v.PrimaryPhone else None,
                    'mobile': v.Mobile.FreeFormNumber if v.Mobile else None,
                    'billing_address': self._format_address(v.BillAddr),
                    'tax_id': v.TaxIdentifier if hasattr(v, 'TaxIdentifier') else None,
                    'payment_terms': v.TermRef.name if v.TermRef else None,
                    'balance': float(v.Balance) if v.Balance else 0,
                    'opening_balance': float(v.OpenBalance) if hasattr(v, 'OpenBalance') and v.OpenBalance else 0,
                    'account_number': v.AcctNum if hasattr(v, 'AcctNum') else None,
                    'notes': v.Notes if hasattr(v, 'Notes') else None,
                    'is_active': v.Active,
                    'is_1099': v.Vendor1099 if hasattr(v, 'Vendor1099') else False,
                    'created_at': v.MetaData.get('CreateTime') if v.MetaData else None,
                    'updated_at': v.MetaData.get('LastUpdatedTime') if v.MetaData else None
                })
            return {'success': True, 'vendors': vendor_list, 'count': len(vendor_list)}
        except Exception as e:
            logger.error(f"Error fetching vendors: {e}")
            return {'success': False, 'error': str(e)}
    def get_vendor_details(self, realm_id, vendor_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            vendor = Vendor.get(vendor_id, qb=qb_client)
            bills = self.get_vendor_bills(realm_id, vendor_id)
            purchase_orders = self.get_vendor_purchase_orders(
                realm_id, vendor_id)
            return {
                'success': True,
                'vendor': {
                    'id': vendor.Id,
                    'display_name': vendor.DisplayName,
                    'company_name': vendor.CompanyName,
                    'email': vendor.PrimaryEmailAddr.Address if vendor.PrimaryEmailAddr else None,
                    'phone': vendor.PrimaryPhone.FreeFormNumber if vendor.PrimaryPhone else None,
                    'billing_address': self._format_address(vendor.BillAddr),
                    'balance': float(vendor.Balance) if vendor.Balance else 0,
                    'is_active': vendor.Active
                },
                'bills': bills.get('bills', []),
                'purchase_orders': purchase_orders.get('purchase_orders', [])
            }
        except Exception as e:
            logger.error(f"Error fetching vendor details: {e}")
            return {'success': False, 'error': str(e)}
    def create_vendor(self, realm_id, vendor_data):
        try:
            qb_client = self.get_qb_client(realm_id)
            vendor = Vendor()
            vendor.DisplayName = vendor_data.get('display_name')
            vendor.CompanyName = vendor_data.get('company_name')
            vendor.GivenName = vendor_data.get('first_name')
            vendor.FamilyName = vendor_data.get('last_name')
            if vendor_data.get('email'):
                from quickbooks.objects.base import EmailAddress
                vendor.PrimaryEmailAddr = EmailAddress()
                vendor.PrimaryEmailAddr.Address = vendor_data.get('email')
            if vendor_data.get('phone'):
                from quickbooks.objects.base import PhoneNumber
                vendor.PrimaryPhone = PhoneNumber()
                vendor.PrimaryPhone.FreeFormNumber = vendor_data.get('phone')
            if vendor_data.get('billing_address'):
                vendor.BillAddr = self._create_address(
                    vendor_data.get('billing_address'))
            if vendor_data.get('account_number'):
                vendor.AcctNum = vendor_data.get('account_number')
            if vendor_data.get('tax_id'):
                vendor.TaxIdentifier = vendor_data.get('tax_id')
            vendor.save(qb=qb_client)
            return {'success': True, 'vendor_id': vendor.Id, 'message': 'Vendor created successfully'}
        except Exception as e:
            logger.error(f"Error creating vendor: {e}")
            return {'success': False, 'error': str(e)}
    def get_all_bills(self, realm_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            bills = Bill.all(qb=qb_client, max_results=1000)
            bill_list = []
            for b in bills:
                line_items = self._parse_bill_lines(b.Line)
                bill_list.append({
                    'id': b.Id,
                    'doc_number': b.DocNumber,
                    'vendor_id': b.VendorRef.value if b.VendorRef else None,
                    'vendor_name': b.VendorRef.name if b.VendorRef else None,
                    'line_items': line_items,
                    'total_amount': float(b.TotalAmt) if b.TotalAmt else 0,
                    'balance_due': float(b.Balance) if b.Balance else 0,
                    'payment_status': 'Paid' if float(b.Balance or 0) == 0 else 'Unpaid',
                    'issue_date': str(b.TxnDate) if b.TxnDate else None,
                    'due_date': str(b.DueDate) if b.DueDate else None,
                    'memo': b.PrivateNote,
                    'created_at': str(b.MetaData.get("CreateTime")) if b.MetaData else None
                })
            return {'success': True, 'bills': bill_list, 'count': len(bill_list)}
        except Exception as e:
            logger.error(f"Error fetching bills: {e}")
            return {'success': False, 'error': str(e)}
    def get_vendor_bills(self, realm_id, vendor_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            bills = Bill.filter(VendorRef=vendor_id, qb=qb_client)
            bill_list = []
            for b in bills:
                bill_list.append({
                    'id': b.Id,
                    'doc_number': b.DocNumber,
                    'total_amount': float(b.TotalAmt) if b.TotalAmt else 0,
                    'balance_due': float(b.Balance) if b.Balance else 0,
                    'payment_status': 'Paid' if float(b.Balance or 0) == 0 else 'Unpaid',
                    'issue_date': str(b.TxnDate) if b.TxnDate else None,
                    'due_date': str(b.DueDate) if b.DueDate else None,
                    'line_items': self._parse_bill_lines(b.Line)
                })
            return {'success': True, 'bills': bill_list}
        except Exception as e:
            logger.error(f"Error fetching vendor bills: {e}")
            return {'success': False, 'error': str(e)}
    def get_all_purchase_orders(self, realm_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            pos = PurchaseOrder.all(qb=qb_client, max_results=1000)
            po_list = []
            for po in pos:
                line_items = self._parse_po_lines(po.Line)
                po_list.append({
                    'id': po.Id,
                    'doc_number': po.DocNumber,
                    'vendor_id': po.VendorRef.value if po.VendorRef else None,
                    'vendor_name': po.VendorRef.name if po.VendorRef else None,
                    'line_items': line_items,
                    'total_amount': float(po.TotalAmt) if po.TotalAmt else 0,
                    'status': po.POStatus if hasattr(po, 'POStatus') else 'Open',
                    'issue_date': str(po.TxnDate) if po.TxnDate else None,
                    'expected_date': str(po.DueDate) if po.DueDate else None,
                    'ship_to': self._format_address(po.ShipAddr),
                    'memo': po.PrivateNote,
                    'created_at': str(po.MetaData.get("CreateTime")) if po.MetaData else None
                })
            return {'success': True, 'purchase_orders': po_list, 'count': len(po_list)}
        except Exception as e:
            logger.error(f"Error fetching purchase orders: {e}")
            return {'success': False, 'error': str(e)}
    def get_vendor_purchase_orders(self, realm_id, vendor_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            pos = PurchaseOrder.filter(VendorRef=vendor_id, qb=qb_client)
            po_list = []
            for po in pos:
                po_list.append({
                    'id': po.Id,
                    'doc_number': po.DocNumber,
                    'total_amount': float(po.TotalAmt) if po.TotalAmt else 0,
                    'status': po.POStatus if hasattr(po, 'POStatus') else 'Open',
                    'issue_date': str(po.TxnDate) if po.TxnDate else None,
                    'line_items': self._parse_po_lines(po.Line)
                })
            return {'success': True, 'purchase_orders': po_list}
        except Exception as e:
            logger.error(f"Error fetching vendor purchase orders: {e}")
            return {'success': False, 'error': str(e)}
    def get_all_items(self, realm_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            items = Item.all(qb=qb_client, max_results=1000)
            item_list = []
            for item in items:
                item_data = {
                    'id': item.Id,
                    'name': item.Name,
                    'full_name': item.FullyQualifiedName,
                    'type': item.Type,
                    'sku': item.Sku if hasattr(item, 'Sku') else None,
                    'description': item.Description,
                    'unit_price': float(item.UnitPrice) if item.UnitPrice else 0,
                    'purchase_cost': float(item.PurchaseCost) if item.PurchaseCost else 0,
                    'is_active': item.Active,
                    'taxable': item.Taxable if hasattr(item, 'Taxable') else False,
                    'track_qty_on_hand': item.TrackQtyOnHand if hasattr(item, 'TrackQtyOnHand') else False,
                    'qty_on_hand': float(item.QtyOnHand) if hasattr(item, 'QtyOnHand') and item.QtyOnHand else 0,
                    'reorder_point': float(item.ReorderPoint) if hasattr(item, 'ReorderPoint') and item.ReorderPoint else 0,
                    'inv_start_date': str(item.InvStartDate) if hasattr(item, 'InvStartDate') and item.InvStartDate else None,
                    'income_account': item.IncomeAccountRef.name if item.IncomeAccountRef else None,
                    'expense_account': item.ExpenseAccountRef.name if item.ExpenseAccountRef else None,
                    'asset_account': item.AssetAccountRef.name if hasattr(item, 'AssetAccountRef') and item.AssetAccountRef else None,
                    'asset_value': float(item.QtyOnHand or 0) * float(item.PurchaseCost or 0) if hasattr(item, 'QtyOnHand') else 0,
                    'created_at': str(item.MetaData.get("CreateTime")) if item.MetaData else None,
                    'updated_at': str(item.MetaData.get("LastUpdatedTime")) if item.MetaData and item.MetaData.get("LastUpdatedTime") else None
                }
                item_list.append(item_data)
            inventory_items = [
                i for i in item_list if i['type'] == 'Inventory']
            non_inventory_items = [
                i for i in item_list if i['type'] == 'NonInventory']
            service_items = [i for i in item_list if i['type'] == 'Service']
            total_inventory_value = sum(i['asset_value']
                                        for i in inventory_items)
            total_qty_on_hand = sum(i['qty_on_hand'] for i in inventory_items)
            low_stock_items = [
                i for i in inventory_items if i['qty_on_hand'] <= i['reorder_point']]
            return {
                'success': True,
                'items': item_list,
                'inventory_items': inventory_items,
                'non_inventory_items': non_inventory_items,
                'service_items': service_items,
                'summary': {
                    'total_items': len(item_list),
                    'inventory_count': len(inventory_items),
                    'non_inventory_count': len(non_inventory_items),
                    'service_count': len(service_items),
                    'total_inventory_value': total_inventory_value,
                    'total_qty_on_hand': total_qty_on_hand,
                    'low_stock_count': len(low_stock_items)
                },
                'low_stock_items': low_stock_items
            }
        except Exception as e:
            logger.error(f"Error fetching items: {e}")
            return {'success': False, 'error': str(e)}
    def get_inventory_report(self, realm_id):
        try:
            items_result = self.get_all_items(realm_id)
            if not items_result['success']:
                return items_result
            inventory_items = items_result['inventory_items']
            inventory_by_account = {}
            for item in inventory_items:
                account = item['asset_account'] or 'Unassigned'
                if account not in inventory_by_account:
                    inventory_by_account[account] = {
                        'items': [],
                        'total_qty': 0,
                        'total_value': 0
                    }
                inventory_by_account[account]['items'].append(item)
                inventory_by_account[account]['total_qty'] += item['qty_on_hand']
                inventory_by_account[account]['total_value'] += item['asset_value']
            return {
                'success': True,
                'inventory_items': inventory_items,
                'inventory_by_account': inventory_by_account,
                'summary': items_result['summary'],
                'low_stock_items': items_result['low_stock_items']
            }
        except Exception as e:
            logger.error(f"Error generating inventory report: {e}")
            return {'success': False, 'error': str(e)}
    def create_item(self, realm_id, item_data):
        try:
            qb_client = self.get_qb_client(realm_id)
            income_account = self._get_income_account(qb_client)
            expense_account = self._get_expense_account(qb_client)
            asset_account = self._get_asset_account(qb_client)
            item = Item()
            item.Name = item_data.get('name')
            item.Description = item_data.get('description', '')
            item.Type = item_data.get('type', 'NonInventory')
            item.UnitPrice = item_data.get('unit_price', 0)
            item.PurchaseCost = item_data.get('purchase_cost', 0)
            if item_data.get('sku'):
                item.Sku = item_data.get('sku')
            if item.Type == 'Inventory':
                item.TrackQtyOnHand = True
                item.QtyOnHand = item_data.get('quantity', 0)
                item.InvStartDate = datetime.now().strftime('%Y-%m-%d')
                if item_data.get('reorder_point'):
                    item.ReorderPoint = item_data.get('reorder_point')
                if asset_account:
                    item.AssetAccountRef = {"value": asset_account.Id}
            if income_account:
                item.IncomeAccountRef = {"value": income_account.Id}
            if expense_account:
                item.ExpenseAccountRef = {"value": expense_account.Id}
            item.save(qb=qb_client)
            return {'success': True, 'item_id': item.Id, 'message': 'Item created successfully'}
        except Exception as e:
            logger.error(f"Error creating item: {e}")
            return {'success': False, 'error': str(e)}
    def update_item_quantity(self, realm_id, item_id, new_quantity, adjustment_type='set'):
        try:
            qb_client = self.get_qb_client(realm_id)
            item = Item.get(item_id, qb=qb_client)
            if not item.TrackQtyOnHand:
                return {'success': False, 'error': 'This item does not track inventory'}
            old_quantity = float(item.QtyOnHand or 0)
            if adjustment_type == 'set':
                item.QtyOnHand = new_quantity
            elif adjustment_type == 'add':
                item.QtyOnHand = old_quantity + new_quantity
            elif adjustment_type == 'subtract':
                item.QtyOnHand = old_quantity - new_quantity
            item.save(qb=qb_client)
            return {
                'success': True,
                'item_id': item.Id,
                'old_quantity': old_quantity,
                'new_quantity': float(item.QtyOnHand),
                'message': 'Quantity updated successfully'
            }
        except Exception as e:
            logger.error(f"Error updating item quantity: {e}")
            return {'success': False, 'error': str(e)}
    def get_chart_of_accounts(self, realm_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            accounts = Account.all(qb=qb_client, max_results=1000)
            account_list = []
            for a in accounts:
                account_list.append({
                    'id': a.Id,
                    'name': a.Name,
                    'full_name': a.FullyQualifiedName,
                    'account_type': a.AccountType,
                    'account_subtype': a.AccountSubType if hasattr(a, 'AccountSubType') else None,
                    'classification': a.Classification if hasattr(a, 'Classification') else None,
                    'current_balance': float(a.CurrentBalance) if a.CurrentBalance else 0,
                    'opening_balance': float(a.OpeningBalance) if hasattr(a, 'OpeningBalance') and a.OpeningBalance else 0,
                    'description': a.Description if hasattr(a, 'Description') else None,
                    'is_active': a.Active,
                    'is_sub_account': a.SubAccount if hasattr(a, 'SubAccount') else False,
                    'parent_account': a.ParentRef.name if hasattr(a, 'ParentRef') and a.ParentRef else None,
                    'created_at': str(a.MetaData.get("CreateTime")) if a.MetaData else None
                })
            accounts_by_type = {}
            for account in account_list:
                acc_type = account['account_type']
                if acc_type not in accounts_by_type:
                    accounts_by_type[acc_type] = []
                accounts_by_type[acc_type].append(account)
            total_assets = sum(a['current_balance'] for a in account_list if a['account_type'] in [
                               'Bank', 'Accounts Receivable', 'Other Current Asset', 'Fixed Asset', 'Other Asset'])
            total_liabilities = sum(a['current_balance'] for a in account_list if a['account_type'] in [
                                    'Accounts Payable', 'Credit Card', 'Other Current Liability', 'Long Term Liability'])
            total_equity = sum(a['current_balance']
                               for a in account_list if a['account_type'] == 'Equity')
            total_income = sum(a['current_balance']
                               for a in account_list if a['account_type'] == 'Income')
            total_expenses = sum(a['current_balance'] for a in account_list if a['account_type'] in [
                                 'Expense', 'Other Expense', 'Cost of Goods Sold'])
            return {
                'success': True,
                'accounts': account_list,
                'accounts_by_type': accounts_by_type,
                'summary': {
                    'total_accounts': len(account_list),
                    'total_assets': total_assets,
                    'total_liabilities': total_liabilities,
                    'total_equity': total_equity,
                    'total_income': total_income,
                    'total_expenses': total_expenses,
                    'net_income': total_income - total_expenses
                }
            }
        except Exception as e:
            logger.error(f"Error fetching chart of accounts: {e}")
            return {'success': False, 'error': str(e)}
    def sync_products_to_quickbooks(self, realm_id):
        from .models import products, QuickBooksSyncLog
        try:
            qb_client = self.get_qb_client(realm_id)
            product_list = products.objects(is_active=True)
            synced = 0
            failed = 0
            errors = []
            for product in product_list:
                try:
                    existing_items = Item.filter(
                        Name=product.product_name[:100], qb=qb_client)
                    if existing_items:
                        item = existing_items[0]
                        item.Description = product.short_description or ''
                        item.UnitPrice = float(
                            product.base_price) if product.base_price else 0
                        item.save(qb=qb_client)
                    else:
                        item_data = {
                            'name': product.product_name[:100],
                            'description': product.short_description or '',
                            'type': 'NonInventory',
                            'unit_price': float(product.base_price) if product.base_price else 0,
                            'sku': product.mpn or ''
                        }
                        result = self.create_item(realm_id, item_data)
                        if not result['success']:
                            raise Exception(result['error'])
                    synced += 1
                except Exception as e:
                    failed += 1
                    errors.append({
                        'product_id': str(product.id),
                        'product_name': product.product_name,
                        'error': str(e)
                    })
            QuickBooksSyncLog(
                realm_id=realm_id,
                sync_type='products',
                status='success' if failed == 0 else 'partial',
                records_synced=synced,
                error_message=str(errors) if errors else None
            ).save()
            return {
                'success': True,
                'synced': synced,
                'failed': failed,
                'errors': errors
            }
        except Exception as e:
            logger.error(f"Error syncing products: {e}")
            QuickBooksSyncLog(
                realm_id=realm_id,
                sync_type='products',
                status='failed',
                error_message=str(e)
            ).save()
            return {'success': False, 'error': str(e)}
    def _format_address(self, addr):
        if not addr:
            return None
        return {
            'line1': addr.Line1 if hasattr(addr, 'Line1') else None,
            'line2': addr.Line2 if hasattr(addr, 'Line2') else None,
            'city': addr.City if hasattr(addr, 'City') else None,
            'state': addr.CountrySubDivisionCode if hasattr(addr, 'CountrySubDivisionCode') else None,
            'postal_code': addr.PostalCode if hasattr(addr, 'PostalCode') else None,
            'country': addr.Country if hasattr(addr, 'Country') else None
        }
    def _create_address(self, addr_data):
        from quickbooks.objects.base import Address
        addr = Address()
        addr.Line1 = addr_data.get('line1', '')
        addr.Line2 = addr_data.get('line2', '')
        addr.City = addr_data.get('city', '')
        addr.CountrySubDivisionCode = addr_data.get('state', '')
        addr.PostalCode = addr_data.get('postal_code', '')
        addr.Country = addr_data.get('country', '')
        return addr
    def _parse_invoice_lines(self, lines):
        if not lines:
            return []
        line_items = []
        for line in lines:
            if line.DetailType == 'SalesItemLineDetail' and line.SalesItemLineDetail:
                detail = line.SalesItemLineDetail
                line_items.append({
                    'item_id': detail.ItemRef.value if detail.ItemRef else None,
                    'item_name': detail.ItemRef.name if detail.ItemRef else None,
                    'sku': detail.ItemRef.name if detail.ItemRef else None,
                    'description': line.Description,
                    'quantity': float(detail.Qty) if detail.Qty else 0,
                    'unit_price': float(detail.UnitPrice) if detail.UnitPrice else 0,
                    'amount': float(line.Amount) if line.Amount else 0,
                    'tax_code': detail.TaxCodeRef.value if hasattr(detail, 'TaxCodeRef') and detail.TaxCodeRef else None
                })
        return line_items
    def _parse_bill_lines(self, lines):
        if not lines:
            return []
        line_items = []
        for line in lines:
            if line.DetailType == 'ItemBasedExpenseLineDetail' and line.ItemBasedExpenseLineDetail:
                detail = line.ItemBasedExpenseLineDetail
                line_items.append({
                    'item_id': detail.ItemRef.value if detail.ItemRef else None,
                    'item_name': detail.ItemRef.name if detail.ItemRef else None,
                    'description': line.Description,
                    'quantity': float(detail.Qty) if detail.Qty else 0,
                    'unit_price': float(detail.UnitPrice) if detail.UnitPrice else 0,
                    'amount': float(line.Amount) if line.Amount else 0
                })
            elif line.DetailType == 'AccountBasedExpenseLineDetail' and line.AccountBasedExpenseLineDetail:
                detail = line.AccountBasedExpenseLineDetail
                line_items.append({
                    'account_id': detail.AccountRef.value if detail.AccountRef else None,
                    'account_name': detail.AccountRef.name if detail.AccountRef else None,
                    'description': line.Description,
                    'amount': float(line.Amount) if line.Amount else 0
                })
        return line_items
    def _parse_po_lines(self, lines):
        if not lines:
            return []
        line_items = []
        for line in lines:
            if line.DetailType == 'ItemBasedExpenseLineDetail' and line.ItemBasedExpenseLineDetail:
                detail = line.ItemBasedExpenseLineDetail
                line_items.append({
                    'item_id': detail.ItemRef.value if detail.ItemRef else None,
                    'item_name': detail.ItemRef.name if detail.ItemRef else None,
                    'description': line.Description,
                    'quantity': float(detail.Qty) if detail.Qty else 0,
                    'unit_price': float(detail.UnitPrice) if detail.UnitPrice else 0,
                    'amount': float(line.Amount) if line.Amount else 0
                })
        return line_items
    def _get_payment_status(self, invoice):
        total = float(invoice.TotalAmt) if invoice.TotalAmt else 0
        balance = float(invoice.Balance) if invoice.Balance else 0
        if balance == 0:
            return 'Paid'
        elif balance < total:
            return 'Partial'
        else:
            return 'Unpaid'
    def _get_discount_from_lines(self, lines):
        if not lines:
            return 0
        for line in lines:
            if line.DetailType == 'DiscountLineDetail':
                return float(line.Amount) if line.Amount else 0
        return 0
    def _get_shipping_from_lines(self, lines):
        if not lines:
            return 0
        for line in lines:
            if line.DetailType == 'SalesItemLineDetail':
                if line.SalesItemLineDetail and line.SalesItemLineDetail.ItemRef:
                    if 'shipping' in line.SalesItemLineDetail.ItemRef.name.lower():
                        return float(line.Amount) if line.Amount else 0
        return 0
    def _get_income_account(self, qb_client):
        try:
            accounts = Account.filter(AccountType="Income", qb=qb_client)
            return accounts[0] if accounts else None
        except:
            return None
    def _get_expense_account(self, qb_client):
        try:
            accounts = Account.filter(
                AccountType="Cost of Goods Sold", qb=qb_client)
            return accounts[0] if accounts else None
        except:
            return None
    def get_all_sales_orders(self, realm_id):
        if not realm_id:
            return {
                'success': False,
                'error': 'Realm ID is required',
                'sales_orders': [],
                'count': 0
            }
        try:
            qb_client = self.get_qb_client(realm_id)
            entities_to_try = ['SalesOrder', 'Estimate']
            response = None
            used_entity = None
            for entity in entities_to_try:
                try:
                    query = f"SELECT * FROM {entity} ORDER BY MetaData.CreateTime DESC MAXRESULTS 1000"
                    response = qb_client.query(query)
                    used_entity = entity
                    logger.info(f"Successfully queried {entity}")
                    break
                except Exception as e:
                    if 'Metadata not found' in str(e) or 'Invalid query' in str(e):
                        logger.warning(f"{entity} not available, trying next...")
                        continue
                    else:
                        raise e
            if response is None:
                return {
                    'success': False,
                    'error': 'Neither SalesOrder nor Estimate entities are available for this account',
                    'sales_orders': [],
                    'count': 0
                }
            order_list = []
            if hasattr(response, 'QueryResponse'):
                data = response.QueryResponse.get(used_entity, [])
                for so in data:
                    try:
                        order_list.append({
                            'id': so.get('Id'),
                            'doc_number': so.get('DocNumber'),
                            'customer_id': so.get('CustomerRef', {}).get('value'),
                            'customer_name': so.get('CustomerRef', {}).get('name'),
                            'total_amount': float(so.get('TotalAmt', 0)),
                            'status': so.get('TxnStatus', 'Open'),
                            'issue_date': so.get('TxnDate'),
                            'expiry_date': so.get('ExpirationDate'),  
                            'delivery_date': so.get('ShipDate'),
                            'created_at': so.get('MetaData', {}).get('CreateTime'),
                            'entity_type': used_entity  
                        })
                    except Exception as e:
                        logger.warning(f"Failed to parse {used_entity}: {e}")
                        continue
            return {
                'success': True,
                'sales_orders': order_list,
                'count': len(order_list),
                'entity_type': used_entity
            }
        except Exception as e:
            logger.error(f"Error fetching sales orders: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'sales_orders': [],
                'count': 0
            }
    def _parse_single_sales_order(self,sales_order):
        try:
            if not sales_order:
                return {
                'success':False,
                'error':'Sales Order  is required',
            }
            return {
                "id":str(sales_order.Id) if hasattr(sales_order,'Id') and sales_order.Id else None,
                'doc_number':str(sales_order.DocNumber) if hasattr(sales_order,'DocNumber') and sales_order.DocNumber else None,
                'customer_id':str(sales_order.CustomerRef.value) if hasattr(sales_order,'CustomerRef') and sales_order.CustomerRef else None,
                'customer_name':str(sales_order.CustomerRef.name) if hasattr(sales_order,"DocNumber") and sales_order.CustomerRef else None,
                'line_items':self._parse_sales_order_lines(sales_order.Line) if hasattr(sales_order,'Line')else [],
                'total_amount':float(sales_order.TotalAmt) if hasattr(sales_order,'TotalAmt') and sales_order.TotalAmt else 0.0,
                'status':self._get_sales_order_status(sales_order),
                'issue_date': str(sales_order.TxnDate) if hasattr(sales_order, 'TxnDate') and sales_order.TxnDate else None,
                'delivery_date':str(sales_order.ShipDate)if hasattr(sales_order,'ShipDate') and sales_order.ShipDate else None,
                'shipping_address': self._format_address(sales_order.ShipAddr) if hasattr(sales_order, 'ShipAddr') else None,
                'billing_address':self._format_address(sales_order.BillAddr) if hasattr(sales_order,'BillAddr') else None,
                'memo': str(sales_order.CustomerMemo.value) if hasattr(sales_order, 'CustomerMemo') and sales_order.CustomerMemo else None,
                'created_at': str(sales_order.MetaData.get("CreateTime")) if hasattr(sales_order, 'MetaData') and sales_order.MetaData else None
            }
        except Exception as e:
            logger.warning(f"Failed to parse sales order: {str(e)}")
            return None
    def _parse_sales_order_lines(sele,lines):
        if not lines:
            return []
        line_items=[]
        try:
            for line in lines:
                try:
                    if hasattr(line,'DetailType') and line.DetailType=='SalesItemLineDetail' and hasattr(line,'SalesItemLineDetails'):
                        detail=line.SalesItemDetail
                        line_items.append({
                            'item_id':str(detail.ItemRef.value) if detail.ItemRef else None,
                            'item_name':str(detail.ItemRef.name) if detail.ItemRef else None,
                            'description':str(line.Description) if hasattr(line,'Description') else None,
                            'quantity':float(detail.Qty) if detail.Qty else 0.0,
                            'unit_price':float(detail.UnitPrice) if detail.UnitPrice else 0.0,
                            'amount':float(line.Amount) if hasattr(line,'Amount') and line.Amount else 0.0,
                            'tax_code':str(detail.TaxCodeRef.value) if hasattr(detail,'TaxCodeRef') and detail.TaxCodeRef else None,
                        })
                except Exception as e:
                    logger.debug(f"Failed to parse line item: {str(e)}")
                    continue
            return line_items
        except Exception as e:
            logger.warning(f"Failed to parse line items: {str(e)}")
            return []
    def _get_sales_order_status(self,sales_order):
        try:
            if hasattr(sales_order,'POStatus') and sales_order.POStatus:
                status_map={
                    "Pending":"Open",
                    "Closed":"Closed",
                    "Finalized":"Closed"
                }
                return status_map.get(sales_order.POStatus,'Open')
            if hasattr(sales_order,'ShipStatus') and sales_order.ShipStatus=='Shipped':
                return "Closed"
            return "Open"
        except Exception as e:
            return 'Unknown'  
    def _get_asset_account(self, qb_client):
        try:
            accounts = Account.filter(
                AccountType="Other Current Asset", qb=qb_client)
            for account in accounts:
                if 'Inventory' in account.Name:
                    return account
            return accounts[0] if accounts else None
        except:
            return None
    def get_invoice_details(self, realm_id, invoice_id):
        try:
            qb_client = self.get_qb_client(realm_id)
            invoice = Invoice.get(invoice_id, qb=qb_client)
            line_items = self._parse_invoice_lines(invoice.Line)
            payments = []
            try:
                query = f"SELECT * FROM Payment WHERE Line.LinkedTxn.TxnId = '{invoice_id}'"
                payment_response = qb_client.query(query)
                if hasattr(payment_response, 'QueryResponse'):
                    payment_data = payment_response.QueryResponse.get('Payment', [])
                    for payment in payment_data:
                        payments.append({
                            'id': payment.get('Id'),
                            'payment_date': payment.get('TxnDate'),
                            'amount': float(payment.get('TotalAmt', 0)),
                            'payment_method': payment.get('PaymentMethodRef', {}).get('name'),
                            'payment_ref_number': payment.get('PaymentRefNum'),
                            'deposit_account': payment.get('DepositToAccountRef', {}).get('name')
                        })
            except Exception as e:
                logger.warning(f"Could not fetch payment details for invoice {invoice_id}: {e}")
            invoice_details = {
                'id': invoice.Id,
                'doc_number': invoice.DocNumber,
                'customer_id': invoice.CustomerRef.value if invoice.CustomerRef else None,
                'customer_name': invoice.CustomerRef.name if invoice.CustomerRef else None,
                'customer_email': invoice.BillEmail.Address if invoice.BillEmail else None,
                'billing_address': self._format_address(invoice.BillAddr),
                'shipping_address': self._format_address(invoice.ShipAddr),
                'issue_date': str(invoice.TxnDate) if invoice.TxnDate else None,
                'due_date': str(invoice.DueDate) if invoice.DueDate else None,
                'ship_date': str(invoice.ShipDate) if invoice.ShipDate else None,
                'created_at': str(invoice.MetaData.CreateTime) if invoice.MetaData else None,
                'last_updated_at': str(invoice.MetaData.LastUpdatedTime) if invoice.MetaData and hasattr(invoice.MetaData, 'LastUpdatedTime') else None,
                'subtotal': float(invoice.TotalAmt) - float(invoice.TxnTaxDetail.TotalTax if invoice.TxnTaxDetail and invoice.TxnTaxDetail.TotalTax else 0) if invoice.TotalAmt else 0,
                'tax_amount': float(invoice.TxnTaxDetail.TotalTax) if invoice.TxnTaxDetail and invoice.TxnTaxDetail.TotalTax else 0,
                'discount': self._get_discount_from_lines(invoice.Line),
                'shipping': self._get_shipping_from_lines(invoice.Line),
                'total_amount': float(invoice.TotalAmt) if invoice.TotalAmt else 0,
                'balance_due': float(invoice.Balance) if invoice.Balance else 0,
                'payment_status': self._get_payment_status(invoice),
                'line_items': line_items,
                'line_items_count': len(line_items),
                'memo': invoice.CustomerMemo.value if invoice.CustomerMemo else None,
                'terms': invoice.SalesTermRef.name if invoice.SalesTermRef else None,
                'currency': invoice.CurrencyRef.value if invoice.CurrencyRef else 'USD',
                'exchange_rate': float(invoice.ExchangeRate) if hasattr(invoice, 'ExchangeRate') and invoice.ExchangeRate else 1.0,
                'linked_payments': payments,
                'payment_count': len(payments),
                'is_paid': float(invoice.Balance or 0) == 0,
                'is_partial': 0 < float(invoice.Balance or 0) < float(invoice.TotalAmt or 0),
                'is_overdue': self._is_invoice_overdue(invoice)
            }
            return {
                'success': True,
                'invoice': invoice_details
            }
        except Exception as e:
            logger.error(f"Error fetching invoice details for invoice {invoice_id}: {e}")
            return {'success': False, 'error': str(e)}
        
    def _is_invoice_overdue(self, invoice):
        try:
            if not invoice.DueDate:
                return False
            due_date = datetime.strptime(str(invoice.DueDate), '%Y-%m-%d')
            today = datetime.now().date()
            balance = float(invoice.Balance or 0)
            if balance > 0 and due_date.date() < today:
                return True
            return False
        except:
            return False
