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


        // Tooltip state for truncated app names
        tooltip: {
            visible: false,
            text: '',
            x: 0,
            y: 0
        },
        tooltipTimer: null,
        pendingTooltipApp: null,
        lastMouseX: 0,
        lastMouseY: 0,
        // Applications overview list initialized synchronously from cache
        apps: (function() {
            try {
                const cached = localStorage.getItem('wiko_cached_apps');
                return cached
                    ? JSON.parse(cached).map(app => ({
                        ...app,
                        stream_count: null,
                        live_count: null,
                        problematic_count: null
                    }))
                    : [];
            } catch (e) {
                return [];
            }
        })(),
        isLoadingApps: false,
        appsUnavailable: true,

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

            // Auto-hide floating tooltip on any container scroll
            window.addEventListener('scroll', () => {
                if (this.tooltip.visible || this.tooltipTimer) {
                    this.hideTooltip();
                }
            }, true);
        },

        toggle() {
            this.hideTooltip();
            this.isCollapsed = !this.isCollapsed;
            localStorage.setItem('wiko_sidebar_collapsed', this.isCollapsed);
            if (this.isCollapsed) {
                this.isFlyoutOpen = false;
            }
        },

        toggleMobile() {
            this.hideTooltip();
            this.isMobileOpen = !this.isMobileOpen;
        },

        closeMobile() {
            this.hideTooltip();
            this.isMobileOpen = false;
        },

        toggleAppsDropdown() {
            this.hideTooltip();
            this.isAppsOpen = !this.isAppsOpen;
            localStorage.setItem('wiko_apps_dropdown', this.isAppsOpen);
        },

        openFlyout() {
            this.isFlyoutOpen = true;
        },

        closeFlyout() {
            this.hideTooltip();
            this.isFlyoutOpen = false;
        },

        toggleFlyout() {
            this.hideTooltip();
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
                if (!res.ok) {
                    throw new Error(`Permintaan ringkasan aplikasi gagal: HTTP ${res.status}`);
                }

                const apps = await res.json();
                if (!Array.isArray(apps)) {
                    throw new Error('Respons ringkasan aplikasi bukan berupa daftar');
                }

                this.apps = apps;
                this.appsUnavailable = false;
                try {
                    localStorage.setItem('wiko_cached_apps', JSON.stringify(this.apps));
                } catch (e) {}

                if (this.apps.length > 0 && !this.apps.some(a => a.name === this.selectedApp)) {
                    const params = new URLSearchParams(window.location.search);
                    if (!params.get('app')) {
                        this.selectedApp = this.apps[0].name;
                    }
                }
                window.dispatchEvent(new CustomEvent('ams:apps-loaded', {
                    detail: { apps: this.apps, unavailable: false }
                }));
            } catch (err) {
                this.appsUnavailable = true;
                this.apps = this.apps.map(app => ({
                    ...app,
                    stream_count: null,
                    live_count: null,
                    problematic_count: null
                }));
                try {
                    localStorage.setItem('wiko_cached_apps', JSON.stringify(this.apps));
                } catch (storageError) {}
                window.dispatchEvent(new CustomEvent('ams:apps-loaded', {
                    detail: { apps: this.apps, unavailable: true }
                }));
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
            this.hideTooltip();
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
        },

        // Tooltip event handlers & positioning logic
        handleAppMouseEnter(event, appName) {
            if (this.tooltipTimer) {
                clearTimeout(this.tooltipTimer);
                this.tooltipTimer = null;
            }

            const target = event.currentTarget;
            const nameSpan = target ? target.querySelector('span.truncate') : null;
            // Only show tooltip if the text is genuinely truncated (scrollWidth > clientWidth)
            const isTruncated = nameSpan ? (nameSpan.scrollWidth > nameSpan.clientWidth + 1) : true;
            if (!isTruncated) {
                this.hideTooltip();
                return;
            }

            this.pendingTooltipApp = appName;
            this.lastMouseX = event.clientX;
            this.lastMouseY = event.clientY;

            // Delay 200ms before showing tooltip
            this.tooltipTimer = setTimeout(() => {
                if (this.pendingTooltipApp === appName) {
                    this.showTooltip(appName, this.lastMouseX, this.lastMouseY);
                }
            }, 200);
        },

        handleAppMouseMove(event) {
            this.lastMouseX = event.clientX;
            this.lastMouseY = event.clientY;

            if (this.tooltip.visible) {
                this.updateTooltipPosition(event.clientX, event.clientY);
            }
        },

        handleAppMouseLeave() {
            this.hideTooltip();
        },

        showTooltip(text, clientX, clientY) {
            this.tooltip.text = text;
            this.tooltip.visible = true;
            this.updateTooltipPosition(clientX, clientY);

            // Refine position in the next animation frame once DOM dimensions are measured
            requestAnimationFrame(() => {
                if (this.tooltip.visible) {
                    this.updateTooltipPosition(this.lastMouseX, this.lastMouseY);
                }
            });
        },

        hideTooltip() {
            if (this.tooltipTimer) {
                clearTimeout(this.tooltipTimer);
                this.tooltipTimer = null;
            }
            this.pendingTooltipApp = null;
            this.tooltip.visible = false;
            this.tooltip.text = '';
        },

        updateTooltipPosition(clientX, clientY) {
            const tooltipEl = document.getElementById('sidebar-app-tooltip');
            const width = tooltipEl && tooltipEl.offsetWidth ? tooltipEl.offsetWidth : 160;
            const height = tooltipEl && tooltipEl.offsetHeight ? tooltipEl.offsetHeight : 28;
            const viewportWidth = window.innerWidth;
            const viewportHeight = window.innerHeight;

            let x = clientX + 8;
            let y = clientY + 16;

            // Viewport boundary clamping: prevent clipping on right/bottom edges
            if (x + width + 8 > viewportWidth) {
                x = Math.max(8, viewportWidth - width - 8);
            }
            if (y + height + 8 > viewportHeight) {
                // Flip above the cursor if it would overflow the bottom viewport
                y = Math.max(8, clientY - height - 8);
            }

            this.tooltip.x = x;
            this.tooltip.y = y;
        }
    });
});
