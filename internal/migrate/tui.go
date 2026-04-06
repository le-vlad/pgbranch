package migrate

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("12"))

	phaseStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("10"))

	tableNameStyle = lipgloss.NewStyle().
			Width(30).
			Foreground(lipgloss.Color("7"))

	doneStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("10")).
			Bold(true)

	pendingStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("8"))

	statsBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("12")).
			Padding(0, 1)

	labelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("8"))

	valueStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15"))

	hintStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("8")).
			Italic(true)
)

type PhaseMsg struct {
	Phase string
}

type TableInitMsg struct {
	Table     string
	TotalRows int64
}

type TableProgressMsg struct {
	Table     string
	RowsDelta int64
}

type TableDoneMsg struct {
	Table string
}

type StreamingUpdateMsg struct {
	LSN      string
	Inserts  int64
	Updates  int64
	Deletes  int64
}

type MigrationDoneMsg struct {
	Err error
}

type tickMsg time.Time

type Model struct {
	phase     string
	tables    []tableRow
	tableIdx  map[string]int
	streaming streamingStats
	startTime time.Time
	spinner   spinner.Model
	quitting  bool
	err       error
	width     int
}

type tableRow struct {
	name      string
	total     int64
	copied    int64
	done      bool
	bar       progress.Model
	startTime time.Time
}

type streamingStats struct {
	lsn      string
	inserts  int64
	updates  int64
	deletes  int64
	lastTick time.Time
	prevOps  int64
	opsPerS  float64
}

func NewModel() Model {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("12"))

	return Model{
		phase:     "init",
		tableIdx:  make(map[string]int),
		startTime: time.Now(),
		spinner:   s,
	}
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, tickCmd())
}

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" || msg.String() == "q" {
			m.quitting = true
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width

	case PhaseMsg:
		m.phase = msg.Phase

	case TableInitMsg:
		bar := progress.New(
			progress.WithDefaultGradient(),
			progress.WithWidth(30),
			progress.WithoutPercentage(),
		)
		idx := len(m.tables)
		m.tables = append(m.tables, tableRow{
			name:      msg.Table,
			total:     msg.TotalRows,
			bar:       bar,
			startTime: time.Now(),
		})
		m.tableIdx[msg.Table] = idx

	case TableProgressMsg:
		if idx, ok := m.tableIdx[msg.Table]; ok {
			m.tables[idx].copied += msg.RowsDelta
		}

	case TableDoneMsg:
		if idx, ok := m.tableIdx[msg.Table]; ok {
			m.tables[idx].done = true
			m.tables[idx].copied = m.tables[idx].total
		}

	case StreamingUpdateMsg:
		m.streaming.lsn = msg.LSN
		m.streaming.inserts = msg.Inserts
		m.streaming.updates = msg.Updates
		m.streaming.deletes = msg.Deletes

	case MigrationDoneMsg:
		m.err = msg.Err
		m.quitting = true
		return m, tea.Quit

	case tickMsg:
		now := time.Time(msg)
		if !m.streaming.lastTick.IsZero() {
			dt := now.Sub(m.streaming.lastTick).Seconds()
			if dt > 0 {
				currentOps := m.streaming.inserts + m.streaming.updates + m.streaming.deletes
				m.streaming.opsPerS = float64(currentOps-m.streaming.prevOps) / dt
				m.streaming.prevOps = currentOps
			}
		}
		m.streaming.lastTick = now
		return m, tickCmd()

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m Model) View() string {
	if m.quitting && m.err != nil {
		return fmt.Sprintf("\n  Error: %s\n\n", m.err)
	}
	if m.quitting {
		return "\n  Migration complete.\n\n"
	}

	var b strings.Builder

	b.WriteString("\n")
	b.WriteString("  " + titleStyle.Render("Continuous Migration"))
	b.WriteString("\n")

	switch m.phase {
	case "validate":
		b.WriteString("  " + phaseStyle.Render("Phase: Validating") + " " + m.spinner.View())
	case "schema":
		b.WriteString("  " + phaseStyle.Render("Phase: Copying Schema") + " " + m.spinner.View())
	case "setup":
		b.WriteString("  " + phaseStyle.Render("Phase: Setting Up Replication") + " " + m.spinner.View())
	case "snapshot":
		b.WriteString("  " + phaseStyle.Render("Phase: Initial Snapshot"))
		b.WriteString("\n\n")
		b.WriteString(m.renderSnapshotTables())
	case "streaming":
		b.WriteString("  " + phaseStyle.Render("Phase: WAL Streaming (live)"))
		b.WriteString("\n\n")
		b.WriteString(m.renderStreamingStats())
	default:
		b.WriteString("  " + phaseStyle.Render("Phase: Initializing") + " " + m.spinner.View())
	}

	elapsed := time.Since(m.startTime).Truncate(time.Second)
	b.WriteString("\n\n")
	b.WriteString("  " + labelStyle.Render(fmt.Sprintf("Elapsed: %s", elapsed)))
	b.WriteString("\n\n")

	return b.String()
}

func (m Model) renderSnapshotTables() string {
	var b strings.Builder

	var totalCopied int64
	for _, t := range m.tables {
		name := tableNameStyle.Render(t.name)

		if t.done {
			b.WriteString(fmt.Sprintf("  %s %s\n",
				name,
				doneStyle.Render("DONE"),
			))
		} else if t.total > 0 && t.copied > 0 {
			pct := float64(t.copied) / float64(t.total)
			bar := t.bar.ViewAs(pct)
			rate := float64(0)
			elapsed := time.Since(t.startTime).Seconds()
			if elapsed > 0 {
				rate = float64(t.copied) / elapsed
			}
			b.WriteString(fmt.Sprintf("  %s %s %s/%s  %s\n",
				name,
				bar,
				formatCount(t.copied),
				formatCount(t.total),
				labelStyle.Render(fmt.Sprintf("%.0f rows/s", rate)),
			))
		} else {
			b.WriteString(fmt.Sprintf("  %s %s\n",
				name,
				pendingStyle.Render("pending"),
			))
		}
		totalCopied += t.copied
	}

	b.WriteString("\n")
	b.WriteString(fmt.Sprintf("  Total: %s rows copied", formatCount(totalCopied)))

	return b.String()
}

func (m Model) renderStreamingStats() string {
	totalOps := m.streaming.inserts + m.streaming.updates + m.streaming.deletes
	uptime := time.Since(m.startTime).Truncate(time.Second)

	content := fmt.Sprintf(
		"%s %s    %s %s\n%s %s   %s %s\n%s %s  %s %s\n%s %s",
		labelStyle.Render("LSN:"),
		valueStyle.Render(m.streaming.lsn),
		labelStyle.Render("Total ops:"),
		valueStyle.Render(formatCount(totalOps)),

		labelStyle.Render("Inserts:"),
		valueStyle.Render(formatCount(m.streaming.inserts)),
		labelStyle.Render("Updates:"),
		valueStyle.Render(formatCount(m.streaming.updates)),

		labelStyle.Render("Deletes:"),
		valueStyle.Render(formatCount(m.streaming.deletes)),
		labelStyle.Render("Throughput:"),
		valueStyle.Render(fmt.Sprintf("%.1f ops/s", m.streaming.opsPerS)),

		labelStyle.Render("Uptime:"),
		valueStyle.Render(uptime.String()),
	)

	box := statsBoxStyle.Render(content)

	return "  " + strings.ReplaceAll(box, "\n", "\n  ") +
		"\n\n  " + hintStyle.Render("Press Ctrl+C to stop")
}

func formatCount(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1_000_000 {
		return fmt.Sprintf("%d,%03d", n/1000, n%1000)
	}
	return fmt.Sprintf("%d,%03d,%03d", n/1_000_000, (n/1000)%1000, n%1000)
}

type PlainLogger struct {
	phase     string
	startTime time.Time
}

func NewPlainLogger() *PlainLogger {
	return &PlainLogger{startTime: time.Now()}
}

func (l *PlainLogger) SetPhase(phase string) {
	l.phase = phase
	elapsed := time.Since(l.startTime).Truncate(time.Second)
	fmt.Printf("[%s] Phase: %s\n", elapsed, phase)
}

func (l *PlainLogger) TableInit(table string, totalRows int64) {
	fmt.Printf("  → %s (%s rows)\n", table, formatCount(totalRows))
}

func (l *PlainLogger) TableProgress(table string, copied, total int64) {
	pct := float64(0)
	if total > 0 {
		pct = float64(copied) / float64(total) * 100
	}
	fmt.Printf("  → %s: %s/%s (%.0f%%)\n", table, formatCount(copied), formatCount(total), pct)
}

func (l *PlainLogger) TableDone(table string) {
	fmt.Printf("  ✓ %s: done\n", table)
}

func (l *PlainLogger) StreamingUpdate(lsn string, inserts, updates, deletes int64) {
	elapsed := time.Since(l.startTime).Truncate(time.Second)
	fmt.Printf("[%s] LSN: %s | I:%d U:%d D:%d\n", elapsed, lsn, inserts, updates, deletes)
}
