class ToUpper:
    @staticmethod
    def execute(text_in):
        x = text_in.upper()
        return x


class TrimWhiteSpace:
    @staticmethod
    def execute(text_in):
        return text_in.strip()
