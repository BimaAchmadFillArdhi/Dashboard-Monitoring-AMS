// Sidebar & Application State Store for Wiko AMS
document.addEventListener('alpine:init', () => {
    Alpine.store('sidebar', {
        // Collapsed state persisted in localStorage
        isCollapsed: localStorage.getItem('wiko_sidebar_collapsed') === 'true',
        
        // Mobile drawer state
        isMobileOpen: false,

        // Current active page ('dashboard' | 'applications' | 'logs')
        activePage: (function() {
            const path = window.location.pathname;
            if (path.startsWith('/applications')) return 'applications';
            if (path.startsWith('/logs')) return 'logs';
            return 'dashboard';
        })(),

        // Applications dropdown in expanded mode
        isAppsOpen: window.location.pathname.startsWith('/applications') 
            ? true 
            : (localStorage.getItem('wiko_apps_dropdown') !== 'false'),

        // Applications flyout popup in collapsed mode
        isFlyoutOpen: false,

        // Real-time app search filter
        appsFilter: '',

        // Add application placeholder modal
        isAddAppModalOpen: false,

        // Applications overview list initialized synchronously from cache
        apps: (function() {
            try {
                const cached = localStorage.getItem('wiko_cached_apps');
                return cached ? JSON.parse(cached) : [];
            } catch (e) {
                return [];
            }
        })(),
        isLoadingApps: false,

        // Selected application
        selectedApp: (function() {
            const params = new URLSearchParams(window.location.search);
            return params.get('app') || 'atcs';
        })(),

        init() {
            const params = new URLSearchParams(window.location.search);
            if (params.get('app')) {
                this.selectedApp = params.get('app');
            }
            this.fetchApps();

            // Handle popstate for browser back/forward buttons seamlessly
            window.addEventListener('popstate', () => {
                const p = new URLSearchParams(window.location.search);
                const app = p.get('app');
                if (app && app !== this.selectedApp) {
                    this.selectedApp = app;
                    window.dispatchEvent(new CustomEvent('ams:app-selected', { detail: { appName: app } }));
                }

                const path = window.location.pathname;
                let page = 'dashboard';
                if (path.startsWith('/applications')) page = 'applications';
                else if (path.startsWith('/logs')) page = 'logs';
                this.activePage = page;

                // Seamlessly swap main content if HTMX is present
                const mainLayout = document.getElementById('main-layout');
                if (window.htmx && mainLayout) {
                    htmx.ajax('GET', window.location.href, {
                        target: '#main-layout',
                        select: '#main-layout',
                        swap: 'outerHTML'
                    }).then(() => {
                        const newLayout = document.getElementById('main-layout');
                        if (newLayout && window.Alpine) {
                            Alpine.initTree(newLayout);
                        }
                    });
                }
            });
        },

        toggle() {
            this.isCollapsed = !this.isCollapsed;
            localStorage.setItem('wiko_sidebar_collapsed', this.isCollapsed);
            if (this.isCollapsed) {
                this.isFlyoutOpen = false;
            }
        },

        toggleMobile() {
            this.isMobileOpen = !this.isMobileOpen;
        },

        closeMobile() {
            this.isMobileOpen = false;
        },

        toggleAppsDropdown() {
            this.isAppsOpen = !this.isAppsOpen;
            localStorage.setItem('wiko_apps_dropdown', this.isAppsOpen);
        },

        openFlyout() {
            this.isFlyoutOpen = true;
        },

        closeFlyout() {
            this.isFlyoutOpen = false;
        },

        toggleFlyout() {
            this.isFlyoutOpen = !this.isFlyoutOpen;
        },

        openAddAppModal() {
            this.isAddAppModalOpen = true;
        },

        closeAddAppModal() {
            this.isAddAppModalOpen = false;
        },

        async fetchApps() {
            this.isLoadingApps = true;
            try {
                const res = await fetch('/api/applications');
                if (res.ok) {
                    this.apps = await res.json();
                    try {
                        localStorage.setItem('wiko_cached_apps', JSON.stringify(this.apps));
                    } catch (e) {}

                    if (this.apps.length > 0 && !this.apps.some(a => a.name === this.selectedApp)) {
                        const params = new URLSearchParams(window.location.search);
                        if (!params.get('app')) {
                            this.selectedApp = this.apps[0].name;
                        }
                    }
                    window.dispatchEvent(new CustomEvent('ams:apps-loaded', { detail: { apps: this.apps } }));
                }
            } catch (err) {
                console.error('Gagal mengambil daftar aplikasi di sidebar:', err);
            } finally {
                this.isLoadingApps = false;
            }
        },

        get filteredApps() {
            const q = this.appsFilter.trim().toLowerCase();
            if (!q) return this.apps;
            return this.apps.filter(app => app.name.toLowerCase().includes(q));
        },

        navigate(url) {
            if (window.location.pathname === url && !window.location.search) {
                return;
            }

            let targetPage = 'dashboard';
            if (url.startsWith('/applications')) {
                targetPage = 'applications';
            } else if (url.startsWith('/logs')) {
                targetPage = 'logs';
            }
            this.activePage = targetPage;
            this.isFlyoutOpen = false;
            this.isMobileOpen = false;

            const mainLayout = document.getElementById('main-layout');
            if (window.htmx && mainLayout) {
                htmx.ajax('GET', url, {
                    target: '#main-layout',
                    select: '#main-layout',
                    swap: 'outerHTML'
                }).then(() => {
                    window.history.pushState({}, '', url);
                    const newLayout = document.getElementById('main-layout');
                    if (newLayout && window.Alpine) {
                        Alpine.initTree(newLayout);
                    }
                }).catch(() => {
                    window.location.href = url;
                });
            } else {
                window.location.href = url;
            }
        },

        selectApp(appName) {
            this.selectedApp = appName;
            this.isFlyoutOpen = false;
            this.isMobileOpen = false;

            if (window.location.pathname.startsWith('/applications')) {
                const url = new URL(window.location.href);
                url.searchParams.set('app', appName);
                window.history.replaceState({}, '', url.toString());

                window.dispatchEvent(new CustomEvent('ams:app-selected', { detail: { appName } }));
            } else {
                const targetUrl = '/applications?app=' + encodeURIComponent(appName);
                this.activePage = 'applications';
                const mainLayout = document.getElementById('main-layout');
                if (window.htmx && mainLayout) {
                    htmx.ajax('GET', targetUrl, {
                        target: '#main-layout',
                        select: '#main-layout',
                        swap: 'outerHTML'
                    }).then(() => {
                        window.history.pushState({}, '', targetUrl);
                        const newLayout = document.getElementById('main-layout');
                        if (newLayout && window.Alpine) {
                            Alpine.initTree(newLayout);
                        }
                    }).catch(() => {
                        window.location.href = targetUrl;
                    });
                } else {
                    window.location.href = targetUrl;
                }
            }
        }
    });
});
