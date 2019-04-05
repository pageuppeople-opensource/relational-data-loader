class __AuditColumnsNames:
    def __init__(self):
        self.audit_column_prefix = 'rdl_'
        self.__changed = False

    def update_audit_column_prefix(self, audit_column_prefix):
        if self.__changed:
            raise RuntimeError("Audit Column Prefix has already been set")
        self.audit_column_prefix = audit_column_prefix
        self.__changed = True

    @property
    def TIMESTAMP(self):
        return f'{self.audit_column_prefix}timestamp'

    @property
    def IS_DELETED(self):
        return f'{self.audit_column_prefix}is_deleted'

    @property
    def CHANGE_VERSION(self):
        return f'{self.audit_column_prefix}change_version'


AuditColumnsNames = __AuditColumnsNames()
