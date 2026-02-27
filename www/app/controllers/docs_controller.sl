# Docs controller - handles documentation pages

class DocsController extends Controller
    # GET /docs
    def index(req)
        return render("docs/index", {
            "title": "Documentation",
            "active_page": "index",
            "layout": "layouts/docs"
        });
    end

    # GET /docs/getting-started
    def getting_started(req)
        return render("docs/getting_started", {
            "title": "Getting Started",
            "active_page": "getting-started",
            "layout": "layouts/docs"
        });
    end

    # GET /docs/api
    def api(req)
        return render("docs/api", {
            "title": "REST API",
            "active_page": "api",
            "layout": "layouts/docs"
        });
    end

    # GET /docs/clustering
    def clustering(req)
        return render("docs/clustering", {
            "title": "Clustering",
            "active_page": "clustering",
            "layout": "layouts/docs"
        });
    end

    # GET /docs/tcp-protocol
    def tcp_protocol(req)
        return render("docs/tcp_protocol", {
            "title": "Redis Protocol",
            "active_page": "tcp-protocol",
            "layout": "layouts/docs"
        });
    end

    # GET /docs/commands
    def commands(req)
        return render("docs/commands", {
            "title": "Command Reference",
            "active_page": "commands",
            "layout": "layouts/docs"
        });
    end

    # GET /docs/persistence
    def persistence(req)
        return render("docs/persistence", {
            "title": "Persistence",
            "active_page": "persistence",
            "layout": "layouts/docs"
        });
    end

    # GET /docs/benchmarks
    def benchmarks(req)
        return render("docs/benchmarks", {
            "title": "Benchmarks",
            "active_page": "benchmarks",
            "layout": "layouts/docs"
        });
    end
end
