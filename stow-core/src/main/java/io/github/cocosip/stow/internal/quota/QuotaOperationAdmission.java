package io.github.cocosip.stow.internal.quota;

public interface QuotaOperationAdmission {

    QuotaOperationAdmission UNRESTRICTED = new QuotaOperationAdmission() {
        @Override
        public void enter() {}

        @Override
        public void exit() {}
    };

    void enter();

    void exit();
}
